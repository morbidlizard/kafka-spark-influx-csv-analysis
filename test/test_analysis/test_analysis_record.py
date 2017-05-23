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
    def test__analysis_call_alert_after_reduce(self, mock_allert_message, mock_time):
        # set up input data structure obtained after transformation and aggregation
        input_data_structure = {"packet_size": 0, "traffic": 1, "ip_size": 2, "ip_size_sum": 3}
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
        mock_influx_reader.read.return_value = [
            {"time": timestamp, "packet_size": 100, "traffic": 200, "ip_size": 200, "ip_size_sum": 300}]

        # create mock for time.time()
        mock_time.return_value = 494388224.4525607

        analysis_record((100, 200, 300, 300), input_data_structure, config.content["time_delta"], config.content[
            "accuracy"], config.content["rule"], mock_allert_message, mock_influx_reader, "mock_measurement")

        # Testing that send_message was called when an anomaly was detected
        self.assertTrue(mock_allert_message.send_message.called,
                        "Failed. The send_message didn't call in analysis_record.")

        # Testing that send_message was called with right parameters
        mock_allert_message.send_message.assert_called_with(timestamp=494388224.4525607,
                                                            param={"field": "ip_size",
                                                                   "lower_bound": 200 * (1 - 5 / 100),
                                                                   "upper_bound": 200 * (1 + 5 / 100),
                                                                   'key': None,
                                                                   "value": 300})

    # @mock.patch('analysis.analysis.time')
    # @mock.patch('analysis.alert_message.IAllertMessage')
    # def test__analysis_call_alert_after_reducebykey(self, mock_allert_message, mock_time):
    #     # set up input data structure obtained after transformation and aggregation
    #     input_data_structure = {"traffic": 0, "ip_size": 1, "ip_size_sum": 2}
    #     # set up structure of config
    #     config = TestConfig(
    #         {
    #             "alert": {
    #                 "method": "stdout",
    #                 "option": {}
    #             },
    #             "time_delta": 20,
    #             "accuracy": 3,
    #             "rule": {
    #                 "ip_size": 5,
    #                 "ip_size_sum": 10,
    #                 "traffic": 15
    #             }
    #
    #         })
    #     # create mock for reader data
    #     mock_influx_reader = Mock()
    #     timestamp = time()
    #     mock_influx_reader.read.return_value = [{"time": timestamp, "country": "Russia" , "traffic": 200, "ip_size": 200, "ip_size_sum": 300} ]
    #
    #     # create mock for time.time()
    #     mock_time.return_value = 494388224.4525607
    #
    #     analysis_record((200, 300, 300), input_data_structure, config.content["time_delta"], config.content[
    #         "accuracy"], config.content["rule"], mock_allert_message, mock_influx_reader, (100,))
    #
    #     # Testing that send_message was called when an anomaly was detected
    #     self.assertTrue(mock_allert_message.send_message.called,
    #                     "Failed. The send message didn't call in analysis_record.")
    #
    #     # Testing that send_message was called with right parameters
    #     mock_allert_message.send_message.assert_called_with(timestamp=494388224.4525607,
    #                                                         param={"field": "ip_size",
    #                                                                "lower_bound": 200 * (1 - 5 / 100),
    #                                                                "upper_bound": 200 * (1 + 5 / 100),
    #                                                                'key': (100,),
    #                                                                "value": 300})

    @mock.patch('analysis.analysis.time')
    @mock.patch('analysis.alert_message.IAllertMessage')
    def test__analysis_not_call_alert(self, mock_allert_message, mock_time):
        # set up input data structure obtained after transformation and aggregation
        input_data_structure = {"packet_size": 0, "traffic": 1, "ip_size": 2, "ip_size_sum": 3}
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
        mock_influx_reader.read.return_value = [
            {"time": timestamp, "packet_size": 100, "traffic": 200, "ip_size": 200, "ip_size_sum": 300}]

        # create mock for time.time()
        mock_time.return_value = 494388224.4525607

        analysis_record((100, 200, 203, 300), input_data_structure, config.content["time_delta"], config.content[
            "accuracy"], config.content["rule"], mock_allert_message, mock_influx_reader, "mock_measurement")
        # Testing that the send_message was NOT called.
        self.assertFalse(mock_allert_message.send_message.called,
                         "Failed. The  send_message was called, but there is no anomaly in the data")
