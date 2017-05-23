import types
from unittest import TestCase, mock
from unittest.mock import Mock, MagicMock
from analysis.analysis import Analysis
from errors import UnsupportedAnalysisFormat
from pyspark.sql.types import StructType, StructField, LongType


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAnalysis(TestCase):
    @mock.patch('analysis.analysis.analysis_record')
    def test_get_analysis_lambda_for_reduce(self, mock_analysis_record):
        # set up input data structure obtained after transformation and aggregation
        input_data_structure = {'rule': [{'key': False, 'func_name': 'Max', 'input_field': 'traffic'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'ip_size_sum'}],
                                'operation_type': 'reduceByKey'}
        # set up structure of config
        config = TestConfig(
            {
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
                "time_delta": 20,
                "accuracy": 3,
                "rule": {
                    "ip_size": 5,
                    "ip_size_sum": 10,
                    "traffic": 15
                }

            })

        detection = Analysis(config.content, Mock(), Mock(),
                             input_data_structure)
        lambda_analysis = detection.get_analysis_lambda()
        self.assertIsInstance(lambda_analysis, types.LambdaType,
                              "Failed. get_analysis_lambda should return a lambda object")

        lambda_analysis((3, 4, 5, 4))
        self.assertTrue(mock_analysis_record.called,
                        "Failed. The analysis_record didn't call in lambda that returned by get_analysis_lambda.")

    @mock.patch('pyspark.rdd.RDD', autospec=True)
    @mock.patch('analysis.analysis.analysis_record')
    def test_get_analysis_lambda_for_reducebykey(self, mock_analysis_record, mock_rdd):
        def mock_foreachPartition(test_lambda):
            test_data = [(1, 2, 3)]
            return list(test_lambda(test_data))

        mock_rdd.foreachPartition.side_effect = mock_foreachPartition

        # set up input data structure obtained after transformation and aggregation
        input_data_structure = {'rule': [{'key': True, 'func_name': '', 'input_field': 'src_mac'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'traffic'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'ip_size_sum'}],
                                'operation_type': 'reduceByKey'}
        # set up structure of config
        config = TestConfig(
            {
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
                "time_delta": 20,
                "accuracy": 3,
                "rule": {
                    "ip_size": 5,
                    "ip_size_sum": 10,
                    "traffic": 15
                }

            })
        mock_historical_data_repository = MagicMock()
        mock_alert_sender = MagicMock()
        detection = Analysis(config.content, mock_historical_data_repository, mock_alert_sender,
                             input_data_structure)

        lambda_analysis = detection.get_analysis_lambda()
        self.assertIsInstance(lambda_analysis, types.LambdaType,
                              "Failed. get_analysis_lambda should return a lambda object")

        lambda_analysis(mock_rdd)
        self.assertTrue(mock_rdd.foreachPartition.called,
                        "Failed. The foreachPartition didn't call in lambda that returned by get_analysis_lambda.")
        self.assertTrue(mock_analysis_record.called,
                        "Failed. The mock_analysis_record didn't call in lambda that returned by get_analysis_lambda.")
        self.assertEqual(mock_analysis_record.call_args[0], ((2, 3), {'ip_size': 1, 'traffic': 0, 'ip_size_sum': 2}, 20, 3,
                                                             config.content["rule"], mock_alert_sender,
                                                             mock_historical_data_repository, "mock", ['src_mac'], 1),
                         "Failed. The function analysis_record is called with invalid arguments")

    def test__parse_config(self):
        # set up input data structure obtained after transformation and aggregation
        input_data_structure = {'rule': [{'key': True, 'func_name': '', 'input_field': 'src_ip'},
                                         {'key': True, 'func_name': '', 'input_field': 'src_mac'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size_sum'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'traffic'}],
                                'operation_type': 'reduceByKey'}
        # set up structure of config
        config = TestConfig(
            {
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
                "time_delta": 20,
                "accuracy": 3,
                "rule": {
                    "ip_size": 5,
                    "ip_size_sum": 10,
                    "traffic": 15
                }

            })
        detection = Analysis(config.content, Mock(), Mock(),
                             input_data_structure)
        self.assertEqual(detection._time_delta, config.content["time_delta"],
                         "Error: Field _time_delta of the object class Analysis not equal time_delta from config ")
        self.assertEqual(detection._accuracy, config.content["accuracy"],
                         "Error: Field _accuracy of the object class Analysis not equal time_delta from config ")
        self.assertEqual(detection._rule, config.content["rule"],
                         "Error: Field _rule of the object class Analysis not equal time_delta from config ")

    def test__validation(self):
        # set up input data structure obtained after transformation and aggregation
        input_data_structure = {'rule': [{'key': True, 'func_name': '', 'input_field': 'src_ip'},
                                         {'key': True, 'func_name': '', 'input_field': 'src_mac'},
                                         {'key': False, 'func_name': 'Max', 'input_field': 'ip_size_sum'},
                                         {'key': False, 'func_name': 'Sum', 'input_field': 'traffic'}],
                                'operation_type': 'reduceByKey'}
        # set up structure of config
        config = TestConfig(
            {
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
                "time_delta": 20,
                "accuracy": 3,
                "rule": {
                    "ip_size": 5,
                    "ip_size_sum": 10,
                    "traffic": 15
                }

            })

        with self.assertRaises(UnsupportedAnalysisFormat) as context:
            Analysis(config.content, Mock(), Mock(), input_data_structure)

        self.assertTrue("An error in the analysis rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")
