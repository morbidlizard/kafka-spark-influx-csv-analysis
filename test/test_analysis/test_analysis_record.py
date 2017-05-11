from time import time
from unittest import TestCase, mock
from unittest.mock import Mock

from pyspark.sql.types import StructField, LongType, StructType

from analysis.analysis import Analysis, analysis_record


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAnalysisRecord(TestCase):
    @mock.patch('analysis.analysis.time')
    @mock.patch('analysis.alert_message.IAllertMessage')
    def test__analysis_call_alert(self, mock_allert_message, mock_time):
        # set up input data structure obtained after transformation and aggregation
        input_data_structure = StructType([StructField("packet_size", LongType()),
                                           StructField("traffic", LongType()),
                                           StructField("ip_size", LongType()),
                                           StructField("ip_size_sum", LongType()),
                                           ])
        # set up structure of config
        config = TestConfig(
            {
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
        # create mock for reader data
        mock_influx_reader = Mock()
        timestamp = time()
        mock_influx_reader.read.return_value = ([(timestamp, 100, 200, 200, 300)])

        # create mock for time.time()
        mock_time.return_value = 494388224.4525607

        analysis_record((100, 200, 300, 300), input_data_structure, config.content["time_delta"], config.content[
            "accuracy"], config.content["rule"], mock_allert_message, mock_influx_reader)
        # detection = Analysis(config.content, mock_influx_reader, mock_allert_message, input_data_structure)
        # detection._analysis((100, 200, 300, 300))

        # Testing that send_message was called when an anomaly was detected
        self.assertTrue(mock_allert_message.send_message.called, "Failed alert. The send message didn't call.")

        # Testing that send_message was called with right parameters
        mock_allert_message.send_message.assert_called_with(timestamp=494388224.4525607,
                                                            param=[{"parameter": "ip_size",
                                                                    "lower_bound": 200 * (1 - 5 / 100),
                                                                    "upper_bound": 200 * (1 + 5 / 100), "value": 300}])

    @mock.patch('analysis.analysis.time')
    @mock.patch('analysis.alert_message.IAllertMessage')
    def test__analysis_not_call_alert(self, mock_allert_message, mock_time):
        # set up input data structure obtained after transformation and aggregation
        input_data_structure = StructType([StructField("packet_size", LongType()),
                                           StructField("traffic", LongType()),
                                           StructField("ip_size", LongType()),
                                           StructField("ip_size_sum", LongType()),
                                           ])
        # set up structure of config
        config = TestConfig(
            {
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
        # create mock for reader data
        mock_influx_reader = Mock()
        timestamp = time()
        mock_influx_reader.read.return_value = ([(timestamp, 100, 200, 200, 300)])

        # create mock for time.time()
        mock_time.return_value = 494388224.4525607

        analysis_record((100, 200, 203, 300), input_data_structure, config.content["time_delta"], config.content[
            "accuracy"], config.content["rule"], mock_allert_message, mock_influx_reader)
        # Testing that the send_message was NOT called.
        self.assertFalse(mock_allert_message.send_message.called, "Failed to not send message if not detect anomaly.")


# def test_analysis_record(self):
#     self.fail()
