class IAnalysis(object):
    def __init__(self, config, historical_data_repository, alert_sender, data_structure):
        self._config = config
        self._historical_data_repository = historical_data_repository
        self._alert_sender = alert_sender
        self._data_structure = data_structure

    def get_analysis_lambda(self):
        raise NotImplementedError("get_analysis_lambda method should be overrided!")
