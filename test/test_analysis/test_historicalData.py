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

from unittest import TestCase
from unittest.mock import MagicMock, patch
import nanotime
from analysis.historical_data import HistoricalData


class TestHistoricalData(TestCase):
    def setUp(self):
        self._input_data_structure = {'rule': [{'key': True, 'func_name': '', 'input_field': 'ip'},
                                               {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                               {'key': False, 'func_name': 'Sum', 'input_field': 'ip_size_sum'}],
                                      'operation_type': 'reduceByKey'}
        self._enumerate_output_aggregation_field = {"ip_size_sum": 1, "ip_size": 0}

        self._key_fields_name = list(map(lambda x: x["input_field"],
                                         filter(lambda x: x["key"], self._input_data_structure["rule"])))
        self._accuracy = 2
        self.__batch_duration = 10

    def test_set_zero_value(self):
        historical_data_repository_singleton = MagicMock()
        historical_data = HistoricalData(historical_data_repository_singleton, self._enumerate_output_aggregation_field,
                                         "test_measurement", self._accuracy, self._key_fields_name,
                                         self.__batch_duration)

        historical_data.set_zero_value((1111, 2222))

        self.assertDictEqual(historical_data._zero_value, {"ip_size": 1111, "ip_size_sum": 2222})
        # self.fail()

    def test_set_key(self):
        historical_data_repository_singleton = MagicMock()
        historical_data = HistoricalData(historical_data_repository_singleton, self._enumerate_output_aggregation_field,
                                         "test_measurement", self._accuracy, self._key_fields_name,
                                         self.__batch_duration)
        historical_data.set_key(("8.8.8.8",))

        self.assertEqual(historical_data._key, ("8.8.8.8",))
        # self.fail()

    @patch('analysis.historical_data.time')
    def test_index(self, mock_time):
        mock_time.return_value = 1000.000
        historical_data_repository_singleton = MagicMock()

        def mock_read(measurement, begin_time_interval, end_time_interval, influx_key):
            nano_timestamp, nano_delta = nanotime.timestamp(mock_time()).nanoseconds(), \
                                         nanotime.timestamp((self.__batch_duration)).nanoseconds()
            if (begin_time_interval < nano_timestamp - nano_delta) and \
                    (end_time_interval > nano_timestamp - nano_delta):
                return [{"time": nano_timestamp - nano_delta, "ip_size": 1111, "ip_size_sum": 1111}]
            elif (begin_time_interval < nano_timestamp - 2 * nano_delta) and \
                    (end_time_interval > nano_timestamp - 2 * nano_delta):
                return [{"time": nano_timestamp - 2 * nano_delta, "ip_size": 2222, "ip_size_sum": 2222}]
            elif (begin_time_interval < nano_timestamp - 3 * nano_delta) and \
                    (end_time_interval > nano_timestamp - 3 * nano_delta):
                return [{"time": nano_timestamp - 3 * nano_delta, "ip_size": 3333, "ip_size_sum": 3333}]
            else:
                return []

        historical_data_repository_singleton.read.side_effect = mock_read
        historical_data = HistoricalData(historical_data_repository_singleton, self._enumerate_output_aggregation_field,
                                         "test_measurement", self._accuracy, self._key_fields_name,
                                         self.__batch_duration)
        historical_data.set_zero_value((1111, 2222))
        historical_data.set_key(("8.8.8.8",))

        self.assertDictEqual(historical_data[0], {"ip_size": 1111, "ip_size_sum": 2222, 'key': ('8.8.8.8',)},
                             "Error in overload __getitem__")

        self.assertDictEqual(historical_data[3], {"ip_size": 3333, "ip_size_sum": 3333, 'key': ('8.8.8.8',),
                                                  "time": nanotime.timestamp(
                                                      mock_time()).nanoseconds() - 3 * nanotime.timestamp(
                                                      (self.__batch_duration)).nanoseconds()},
                             "Error in overload __getitem__")

        self.assertDictEqual(historical_data[4], {}, "Error in overload __getitem__")
