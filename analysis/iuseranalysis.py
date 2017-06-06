class IUserAnalysis(object):
    def __init__(self, option, name):
        self._option = option
        self.name = name

    def analysis(self, historical_data, alert_sender):
        raise NotImplementedError("analysis method should be overrided!")
