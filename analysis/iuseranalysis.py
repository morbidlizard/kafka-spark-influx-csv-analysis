class IUserAnalysis(object):
    def __init__(self, option):
        self._option = option

    def analysis(self, historical_data, alert_sender):
        raise NotImplementedError("analysis method should be overrided!")
