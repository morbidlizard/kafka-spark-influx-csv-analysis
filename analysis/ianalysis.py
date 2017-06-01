class IAnalysis(object):
    def __init__(self, config, historical_data_repository, alert_sender, data_structure):
        self._config = config
        self._historical_data_repository = historical_data_repository
        self._alert_sender = alert_sender
        self._input_fields = dict(map(lambda x: reversed(x), enumerate(
            map(lambda x: x["input_field"], filter(lambda x: not x["key"], data_structure["rule"])))))
        self._key_fields_name = map(lambda x: x["input_field"], filter(lambda x: x["key"], data_structure["rule"]))
    def get_analysis_lambda(self):
        raise NotImplementedError("get_analysis_lambda method should be overrided!")