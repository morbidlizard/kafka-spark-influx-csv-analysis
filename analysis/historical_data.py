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

import logging
from time import time
import nanotime


class HistoricalData:
    def __init__(self, historical_data_repository_singleton, data_structure, measurement, accuracy, key_fields_name,
                 batch_duration):
        self._historical_data_repository_singleton = historical_data_repository_singleton
        self._data_structure = data_structure
        self._measurement = measurement
        self._accuracy = accuracy
        self._key_fields_name = key_fields_name
        self._batch_duration = batch_duration

    def set_zero_value(self, value):
        fields = dict(map(lambda x: (x, value[self._data_structure[x]]), self._data_structure.keys()))
        self._zero_value = fields

    def set_key(self, key):
        self._key = key

    def __getitem__(self, index):
        if index == 0:
            self._zero_value["key"] = self._key
            return self._zero_value
        else:
            if self._accuracy >= self._batch_duration:
                logging.warning("Current accuracy {} is more or equal batch duration {}. You can get incorrect "
                                "results of analysis in this case ".format(self._accuracy, self._batch_duration))
            nano_timestamp, nano_delta, nano_accuracy = nanotime.timestamp(time()), nanotime.seconds(
                index * self._batch_duration), nanotime.seconds(self._accuracy)

            begin_time_interval = nano_timestamp - nano_delta - nano_accuracy
            end_time_interval = nano_timestamp - nano_delta + nano_accuracy
            influx_key = None
            if self._key:
                influx_key = dict(map(lambda x, y: (x, y), self._key_fields_name, self._key))
            historical_values = self._historical_data_repository_singleton.read(self._measurement,
                                                                                begin_time_interval.nanoseconds(),
                                                                                end_time_interval.nanoseconds(),
                                                                                influx_key)
            if historical_values:
                historical_values = sorted(historical_values, key=lambda point: point["time"], reverse=True)[0]
                historical_values["key"] = self._key
                return historical_values
            else:
                return {}
