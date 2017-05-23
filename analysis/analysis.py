from time import time
import nanotime
from errors import UnsupportedAnalysisFormat
from pyspark.rdd import RDD
from analysis.ianalysis import IAnalysis


class Analysis(IAnalysis):
    """
    The Analysis class implements functionality for detecting anomalies in aggregation data.
    """

    def __init__(self, config, historical_data_repository, alert_sender, data_structure):
        """
        Constructor
        :param config: dictionary with configuration parameters
        :param historical_data_repository: An object of some class that implements the "read" method. 
        The "read" method must take tree arguments (start timestamp, end timestamp, key) and return a list of the data 
        tuple.
        :param alert_sender: An object of some class (Inheritance from AllertMessage) that implements the "send_message" 
        method.
        :param data_structure: Structure of the input data after the aggregation operation. 
        For example {'traffic': 1, 'packet_size': 0}.  This meansthat in tuple of data field 'traffic' has index 1 and 
        field 'packet_size' has index 0.
        """

        IAnalysis.__init__(self, config, historical_data_repository, alert_sender, data_structure)
        self._parse_config()
        self._validation()

    def _parse_config(self):
        self._time_delta = self._config["time_delta"]
        self._accuracy = self._config["accuracy"]
        self._rule = self._config["rule"]

    def _validation(self):
        input_field_set = set(self._input_fields.keys())
        rule_field_set = set(self._rule.keys())
        intersection_set = rule_field_set - input_field_set
        if intersection_set:
            raise UnsupportedAnalysisFormat("An error in the analysis rule. The field {} is not contained in set of "
                                            "fields after aggregation operation".format(intersection_set))

    def get_analysis_lambda(self):
        """
        Creates a lambda function that analyzes data after aggregation
        :return: lambda function under tuple or rdd object 
        """

        data_structure = self._input_fields
        key_fields_name = list(self._key_fields_name)
        time_delta = self._time_delta
        accuracy = self._accuracy
        rule = self._rule
        alert_sender_singleton = self._alert_sender
        historical_data_repository_singleton = self._historical_data_repository

        measurement = self._config["historical"]["influx_options"]["measurement"]

        def run_necessary_lambda(rdd_or_object):
            if isinstance(rdd_or_object, RDD):
                rdd_or_object.foreachPartition(
                    lambda iterator: map(
                        lambda record: analysis_record(record[1:], data_structure, time_delta, accuracy, rule,
                                                       alert_sender_singleton, historical_data_repository_singleton,
                                                       measurement, key_fields_name, record[0]), iterator))
            else:
                analysis_record(rdd_or_object, data_structure, time_delta, accuracy, rule,
                                alert_sender_singleton, historical_data_repository_singleton, measurement)

        return lambda rdd_or_object: run_necessary_lambda(rdd_or_object)


def analysis_record(input_tuple_value, data_structure, time_delta, accuracy, rule, alert_sender_singleton,
                    historical_data_repository_singleton, measurement, key_fields_name=None, key=None):
    """
    Checks data for deviations
    :param key: This is the key to the data aggregation
    :param input_tuple_value: input data to check
    :return:
    """
    nano_timestamp, nano_delta, nano_accuracy = nanotime.timestamp(time()), nanotime.seconds(
        time_delta), nanotime.seconds(accuracy)

    begin_time_interval = nano_timestamp - nano_delta - nano_accuracy
    end_time_interval = nano_timestamp - nano_delta + nano_accuracy

    # create a dictionary with a key equal to the name of field from rule and a value equal to index in the
    # input tuple nfti (name field to index)
    name_to_index = dict(map(lambda x: (x, data_structure[x]), rule.keys()))

    # call for real historical data
    influx_key = None
    if key:
        influx_key = dict(map(lambda x, y: (x, y), key_fields_name, key))


    historical_values = historical_data_repository_singleton.read(measurement, begin_time_interval.nanoseconds(),
                                                                  end_time_interval.nanoseconds(), influx_key)

    if historical_values:
        historical_values = sorted(historical_values, key=lambda point: point["time"])
        # we analyse only first value from data, because next values will analysed in next moments
        for field in rule.keys():
            lower_bound = historical_values[0][field] * (1 - float(rule[field]) / 100)
            upper_bound = historical_values[0][field] * (1 + float(rule[field]) / 100)
            current_value = input_tuple_value[name_to_index[field]]
            if (current_value < lower_bound) or (current_value > upper_bound):
                alert_sender_singleton.send_message(timestamp=time(),
                                                    param={"key": key,
                                                           "field": field,
                                                           "lower_bound": lower_bound,
                                                           "upper_bound": upper_bound,
                                                           "value": current_value})
