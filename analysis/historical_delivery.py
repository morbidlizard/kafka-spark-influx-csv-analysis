from influxdb import InfluxDBClient
from .history_data_driver import HistoryDataSingleton


class HistoricalDataDelivery(object):
    def __init__(self, config):
        self._config = config

    def read(self, **kwargs):
        raise NotImplementedError("read method should be overrided!")


class HistoricalDataDeliveryFactory(object):
    def __init__(self, config):
        self._config = config

    def instance_data_delivery(self):
        if self._config["historical"]["method"] == "influx":
            influx_options = self._config["historical"]["influx_options"]
            client = InfluxDBClient(influx_options["host"], influx_options["port"], influx_options["username"],
                                    influx_options["password"], influx_options["database"])
            return HistoryDataSingleton(client)
