from datetime import datetime

from singleton import Singleton


class IAllertMessage(object):
    def __init__(self, config):
        self._config = config

    def send_message(self, **kwargs):
        raise NotImplementedError("send_message method should be overrided!")


class AlertMessageFactory(object):
    def __init__(self, config):
        self._config = config

    def instance_alert(self):
        if self._config["alert"]["method"] == "stdout":
            return StdOutAlert(self._config)



class StdOutAlert(IAllertMessage, metaclass=Singleton):
    def send_message(self, **kwargs):
        string = ''
        parameters = kwargs["param"]
        if parameters["key"]:
            string = string + " Parameter '{}' for key={} out of range [{},{}] and equal {}".format(
                parameters["field"],
                parameters["key"],
                parameters["lower_bound"],
                parameters["upper_bound"],
                parameters["value"])
        else:
            string = string + " Parameter '{}' out of range [{},{}] and equal {}".format(
                parameters["field"],
                parameters["lower_bound"],
                parameters["upper_bound"],
                parameters["value"])
        string = "Time: {}".format(datetime.fromtimestamp(int(kwargs["timestamp"]))) + "." + string
        print(string)
