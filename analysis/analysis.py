from time import time

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
        input_field_set = set(self._data_structure.keys())
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

        data_structure = self._data_structure
        time_delta = self._time_delta
        accuracy = self._accuracy
        rule = self._rule
        alert_sender_singleton = self._alert_sender
        historical_data_repository_singleton = self._historical_data_repository

        def run_necessary_lambda(rdd_or_object):
            if isinstance(rdd_or_object, RDD):
                rdd_or_object.foreachPartition(
                    lambda iterator: map(
                        lambda record: analysis_record(record[1:], data_structure, time_delta, accuracy, rule,
                                                       alert_sender_singleton, historical_data_repository_singleton,
                                                       record[0]), iterator))
            else:
                analysis_record(rdd_or_object, data_structure, time_delta, accuracy, rule,
                                alert_sender_singleton, historical_data_repository_singleton)

        return lambda rdd_or_object: run_necessary_lambda(rdd_or_object)


def analysis_record(input_tuple_value, data_structure, time_delta, accuracy, rule, alert_sender_singleton,
                    historical_data_repository_singleton, key=None):
    """
    Checks data for deviations
    :param key: This is the key to the data aggregation
    :param input_tuple_value: input data to check
    :return:
    """
    current_timestamp = time()
    begin_time_interval = current_timestamp - time_delta - accuracy
    end_time_interval = current_timestamp - time_delta + accuracy

    # create a dictionary with a key equal to the name of field from rule and a value equal to index in the
    # input tuple nfti (name field to index)
    nfti = dict(map(lambda x: (x, data_structure[x]), rule.keys()))
    # call for real historical data
    historical_values = historical_data_repository_singleton.read(begin_time_interval, end_time_interval,
                                                                  input_tuple_value, key)
    if historical_values:
        historical_values = sorted(historical_values, key=lambda x: x[0])
        if key:
            historical_values = historical_values[0][2:]
        else:
            historical_values = historical_values[0][1:]
        for field in rule.keys():
            lower_bound = historical_values[nfti[field]] * (1 - float(rule[field]) / 100)
            upper_bound = historical_values[nfti[field]] * (1 + float(rule[field]) / 100)
            current_value = input_tuple_value[nfti[field]]
            if (current_value < lower_bound) or (current_value > upper_bound):
                alert_sender_singleton.send_message(timestamp=time(),
                                                    param={"key": key,
                                                           "field": field,
                                                           "lower_bound": lower_bound,
                                                           "upper_bound": upper_bound,
                                                           "value": current_value})
