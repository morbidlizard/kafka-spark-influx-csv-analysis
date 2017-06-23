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

import types
from unittest import TestCase
from unittest.mock import MagicMock, patch
import sys
from analysis.alert_message import IAllertMessage
from analysis.analysis_factory import AnalysisFactory


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAnalysisFactory(TestCase):
    def setUp(self):
        # set up structure of config
        self._config = TestConfig(
            {
                "input": {
                    "options": {
                        "batchDuration": 4
                    }
                },
                "analysis": {
                    "historical": {
                        "method": "influx",
                        "influx_options": {
                            "measurement": "mock"
                        }
                    },
                    "alert": {
                        "method": "stdout",
                        "option": {}
                    },
                    "accuracy": 3,
                    "rule": [
                        {
                            "module": "MockAnalysis",
                            "name": "MockAnalysis1",
                            "options": {
                                "deviation": {
                                    "packet_size": 5,
                                    "traffic": 8
                                },
                                "batch_number": 1
                            }
                        },
                        {
                            "module": "MockAnalysis",
                            "name": "MockAnalysis2",
                            "options": {
                                "deviation": {
                                    "packet_size": 5,
                                    "traffic": 3
                                },
                                "batch_number": 3
                            }
                        },
                    ]
                }
            })

    @patch('analysis.historical_delivery.HistoricalDataDeliveryFactory.instance_data_delivery')
    def test__init__(self, mock_data_delivery):
        input_data_structure = {'rule': [{'key': False, 'func_name': 'Max', 'input_field': 'traffic'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'ip_size_sum'}],
                                'operation_type': 'reduce'}
        enumerate_output_aggregation_field = {"traffic": 0, "ip_size": 1, "ip_size_sum": 2}

        mock_history_data_singleton = MagicMock()
        mock_data_delivery.return_value = mock_history_data_singleton

        analysis_factory = AnalysisFactory(self._config, input_data_structure,
                                           enumerate_output_aggregation_field)
        self.assertDictEqual(analysis_factory._data_structure_after_aggregation, input_data_structure,
                             "After init of AnalysisFactory field _data_structure_after_aggregation is "
                             "not equal second input arguments ")

        self.assertDictEqual(analysis_factory._input_fields, enumerate_output_aggregation_field,
                             "After init of AnalysisFactory field _input_fields is "
                             "not equal the third input arguments "
                             )

        self.assertIsInstance(analysis_factory._alert_sender, IAllertMessage,
                              "After init of AnalysisFactory the field _alert_sender is "
                              "not instance of IAllertMessage"
                              )
        self.assertListEqual(analysis_factory._key_fields_name, list(map(lambda x: x["input_field"],
                                                                         filter(lambda x: x["key"],
                                                                                input_data_structure["rule"]))),
                             "After init of AnalysisFactory field _key_fields_name have incorrect list of keys")

    @patch('analysis.analysis_factory.HistoricalData', autospec=True)
    @patch('analysis.alert_message.AlertMessageFactory.instance_alert')
    @patch('analysis.historical_delivery.HistoricalDataDeliveryFactory.instance_data_delivery')
    def test_get_analysis_lambda_and_analysis_lambda_reduce(self, mock_data_delivery, mock_alert_factory,
                                                            mock_historical_data):
        input_data_structure = {'rule': [{'key': False, 'func_name': 'Max', 'input_field': 'traffic'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'ip_size_sum'}],
                                'operation_type': 'reduceByKey'}
        enumerate_output_aggregation_field = {"traffic": 0, "ip_size": 1, "ip_size_sum": 2}

        mock_class = MagicMock()
        mock_analysis = MagicMock()
        mock_class.MockAnalysis.return_value = mock_analysis
        mock_analysis.analysis = MagicMock()
        sys.modules['analysis.MockAnalysis'] = mock_class
        mock_history_data_singleton = MagicMock()
        mock_data_delivery.return_value = mock_history_data_singleton
        obj_mock_alert_factory = MagicMock()
        mock_alert_factory.return_value = obj_mock_alert_factory
        obj_mock_historical_data = MagicMock()
        mock_historical_data.return_value = obj_mock_historical_data

        analysis_factory = AnalysisFactory(self._config, input_data_structure, enumerate_output_aggregation_field)

        test_lambda = analysis_factory.get_analysis_lambda()

        self.assertIsInstance(test_lambda, types.LambdaType, "get_analysis_lambda should return lambda function")

        test_lambda((6666, 7777, 8888))

        mock_analysis.analysis.assert_called_with(obj_mock_historical_data, obj_mock_alert_factory)

    @patch('pyspark.rdd.RDD', autospec=True)
    @patch('analysis.analysis_factory.HistoricalData', autospec=True)
    @patch('analysis.alert_message.AlertMessageFactory.instance_alert')
    @patch('analysis.historical_delivery.HistoricalDataDeliveryFactory.instance_data_delivery')
    def test_get_analysis_lambda_and_analysis_lambda_reduceByKey(self, mock_data_delivery, mock_alert_factory,
                                                                 mock_historical_data, mock_rdd):
        input_data_structure = {'rule': [{'key': True, 'func_name': '', 'input_field': 'ip'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'ip_size_sum'}],
                                'operation_type': 'reduceByKey'}
        enumerate_output_aggregation_field = {"ip_size": 1, "ip_size_sum": 2}

        def mock_foreachPartition(test_lambda):
            test_data = [(1, 2, 3)]
            return list(test_lambda(test_data))

        mock_rdd.foreachPartition.side_effect = mock_foreachPartition

        mock_class = MagicMock()
        mock_analysis = MagicMock()
        mock_class.MockAnalysis.return_value = mock_analysis
        mock_analysis.analysis = MagicMock()
        sys.modules['analysis.MockAnalysis'] = mock_class
        mock_history_data_singleton = MagicMock()
        mock_data_delivery.return_value = mock_history_data_singleton
        obj_mock_alert_factory = MagicMock()
        mock_alert_factory.return_value = obj_mock_alert_factory
        obj_mock_historical_data = MagicMock()
        mock_historical_data.return_value = obj_mock_historical_data

        analysis_factory = AnalysisFactory(self._config, input_data_structure, enumerate_output_aggregation_field)

        test_lambda = analysis_factory.get_analysis_lambda()

        self.assertIsInstance(test_lambda, types.LambdaType, "get_analysis_lambda should return lambda function")

        test_lambda(mock_rdd)

        self.assertTrue(mock_rdd.foreachPartition.called,
                        "Failed. The foreachPartition didn't call in lambda that returned by get_analysis_lambda.")

        mock_analysis.analysis.assert_called_with(obj_mock_historical_data, obj_mock_alert_factory)

    @patch('analysis.historical_delivery.HistoricalDataDeliveryFactory.instance_data_delivery')
    def test_get_analysis_lambda_exception(self, mock_data_delivery):
        input_data_structure = {'rule': [{'key': False, 'func_name': 'Max', 'input_field': 'traffic'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'ip_size_sum'}],
                                'operation_type': 'reduceByKey'}
        enumerate_output_aggregation_field = {"traffic": 0, "ip_size": 1, "ip_size_sum": 2}
        self._config = TestConfig(
            {
                "input": {
                    "options": {
                        "batchDuration": 4
                    }
                },
                "analysis": {
                    "historical": {
                        "method": "influx",
                        "influx_options": {
                            "measurement": "mock"
                        }
                    },
                    "alert": {
                        "method": "stdout",
                        "option": {}
                    },
                    "accuracy": 3,
                    "rule": [
                        {
                            "module": "TestException",
                            "name": "TestException1",
                            "option": {
                                "deviation": {
                                    "packet_size": 5,
                                    "traffic": 8
                                },
                                "batch_number": 1
                            }
                        },
                        {
                            "module": "TestException",
                            "name": "TestException2",
                            "option": {
                                "deviation": {
                                    "packet_size": 5,
                                    "traffic": 3
                                },
                                "batch_number": 3
                            }
                        },
                    ]
                    }
                }
            )

        mock_history_data_singleton = MagicMock()
        mock_data_delivery.return_value = mock_history_data_singleton

        analysis_factory = AnalysisFactory(self._config, input_data_structure, enumerate_output_aggregation_field)

        with self.assertRaises(ImportError) as context:
            analysis_factory.get_analysis_lambda()

        self.assertTrue("Missing required analysis" in context.exception.args[0],
                        "Catch exeception, but it differs from test exception")
