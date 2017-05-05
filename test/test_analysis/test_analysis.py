import types
from datetime import datetime
from time import time
from unittest import TestCase, mock
from unittest.mock import Mock

from pyspark.sql.types import StructType, StructField, LongType

from analysis.analysis import Analysis


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAnalysis(TestCase):
    # def test__parse_config(self):
    #     self.fail()

    def test_get_analysis_lambda(self):
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
        detection = Analysis(config.content, Mock(), Mock(),
                             input_data_structure)
        self.assertIsInstance(detection.get_analysis_lambda(), types.LambdaType,
                              "get_analysis_lambda should return a lambda object")

    def test__parse_config(self):
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
        detection = Analysis(config.content, Mock(), Mock(),
                             input_data_structure)
        self.assertEqual(detection._time_delta, config.content["time_delta"],
                         "Error: Field _time_delta of the object class Analysis not equal time_delta from config ")
        self.assertEqual(detection._accuracy, config.content["accuracy"],
                         "Error: Field _time_delta of the object class Analysis not equal time_delta from config ")
        self.assertEqual(detection._rule, config.content["rule"],
                         "Error: Field _time_delta of the object class Analysis not equal time_delta from config ")
