from time import time

from analysis.ianalysis import IAnalysis


class Analysis(IAnalysis):
    """
    The DetectionForReduce class implements functionality for detecting anomalies in aggregation data.
    Input data should be aggregated with a reduce operation and that operation must return tuple of date
    """

    def __init__(self, config, historical_data_repository, alert_sender, data_structure):
        """
        Constructor
        :param config: dictionary with configuration parameters
        :param historical_data_repository: An object of some class that implements the "read" method. 
        The "read" method must take two arguments (start timestamp, end timestamp) and return a list of the data tuple.
        :param alert_sender: An object of some class (Inheritance from AllertMessage) that implements the "send_message" 
        method.
        :param data_structure: Structure of the input data after the aggregation operation 
        """

        IAnalysis.__init__(self, config, historical_data_repository, alert_sender, data_structure)
        self._parse_config()

    def _parse_config(self):
        self._time_delta = self._config["time_delta"]
        self._accuracy = self._config["accuracy"]
        self._rule = self._config["rule"]

    def get_analysis_lambda(self):
        """
        Creates a lambda function that analyzes data after aggregation
        :return: lambda function under tuple
        """

        data_structure = self._data_structure
        time_delta = self._time_delta
        accuracy = self._accuracy
        rule = self._rule
        alert_sender_singleton = self._alert_sender
        historical_data_repository_singleton = self._historical_data_repository
        return lambda tuple_data: analysis_record(tuple_data, data_structure, time_delta, accuracy, rule,
                                                  alert_sender_singleton,
                                                  historical_data_repository_singleton)


def analysis_record(input_tuple_value, data_structure, time_delta, accuracy, rule, alert_sender_singleton,
                    historical_data_repository_singleton):
    """
    Checks data for deviations
    :param input_tuple_value: input data to check
    :return:
    """
    current_timestamp = time()
    begin_time_interval = current_timestamp - time_delta - accuracy
    end_time_interval = current_timestamp - time_delta + accuracy

    # create a dictionary with a key equal to the name of field from rule and a value equal to index in the
    # input tuple nfti (name field to index)
    set_fields_after_tr = list(map(lambda x: x.name, data_structure))
    nfti = dict(map(lambda x: (x, set_fields_after_tr.index(x)), rule.keys()))

    # call for real historical data
    # historical_values = self._historical_data_delivery.read(begin_time_interval, end_time_interval)
    historical_values = historical_data_repository_singleton.read(begin_time_interval, end_time_interval,
                                                                  input_tuple_value)
    if historical_values:
        historical_values = sorted(historical_values, key=lambda x: x[0])
        historical_values = historical_values[0][1:]

        for field in rule.keys():
            lower_bound = historical_values[nfti[field]] * (1 - float(rule[field]) / 100)
            upper_bound = historical_values[nfti[field]] * (1 + float(rule[field]) / 100)
            current_value = input_tuple_value[nfti[field]]
            if (current_value < lower_bound) or (current_value > upper_bound):
                alert_sender_singleton.send_message(timestamp=time(),
                                                    param=[{"parameter": field,
                                                            "lower_bound": lower_bound,
                                                            "upper_bound": upper_bound,
                                                            "value": current_value}])
