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
        for row in kwargs["param"]:
            string = string + " Parameter '{}' out of range [{},{}] and equal {}".format(
                row["parameter"],
                row["lower_bound"],
                row["upper_bound"],
                row["value"])
        string = "Time: {}".format(kwargs["timestamp"]) + "." + string
        print(string)
