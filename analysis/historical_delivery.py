# Copyright 2017, bwsoft management
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
