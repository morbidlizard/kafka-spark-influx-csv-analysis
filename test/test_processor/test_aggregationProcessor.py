from unittest import TestCase
import os
import types

from config_parsing.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from processor.aggregation_processor import AggregationProcessor

traffic = StructField('traffic', LongType())
src_ip = StructField('src_ip', StringType())
packet_size = StructField('packet_size', LongType())

data_struct = StructType([src_ip, packet_size, traffic])

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_reduce_by_key.json"))


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAggregationProcessor(TestCase):
    def test_build_lambda_for_reduce(self):
        test_input_rule = "Min(packet_size),Max(sampling_rate), Sum(traffic)"
        input_data_structure = StructType([StructField("sampling_rate", LongType()),
                                           StructField("packet_size", LongType()),
                                           StructField("traffic", LongType())])
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_processor = AggregationProcessor(config, input_data_structure)

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext
        test_rdd = sc.parallelize([(4, 2, 1), (7, 1, 1), (1, 0, 1), (2, 5, 1), (1, 1, 1)])
        test_aggregation_lambda = test_aggregation_processor.get_aggregation_lambda()

        self.assertIsInstance(test_aggregation_lambda, types.LambdaType, "get_aggregation_lambda should return "
                                                                         "lambda function")

        test_result = test_aggregation_lambda(test_rdd)
        self.assertTupleEqual(test_result, (7, 0, 5), "Error in aggregation operation. Tuple should be equal")

    def test_remove_key_struct_from_aggregation_expression_rule(self):
        config = Config(CONFIG_PATH)
        aggregation_processor = AggregationProcessor(config, data_struct)

        self.assertListEqual(aggregation_processor._aggregation_expression["rule"],
                             [
                                 {
                                     "key": False,
                                     "func_name": "Sum",
                                     "input_field": "traffic"
                                 },
                                 {
                                     "key": False,
                                     "func_name": "Sum",
                                     "input_field": "packet_size"
                                 },
                             ],
                             "Lists should be equal")

    def test_separate_key_from_start(self):
        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        rdd = sc.parallelize([
            ("217.69.143.60", 100, 4000),
            ("217.69.143.60", 100, 4000),
            ("192.168.30.2", 1500, 54000),
            ("192.168.30.2", 200, 3000),
            ("192.168.30.2", 200, 3000)
        ])

        config = Config(CONFIG_PATH)
        aggregation_processor = AggregationProcessor(config, data_struct)

        separate_key = aggregation_processor._get_separate_key_lambda()
        result = separate_key(rdd)
        self.assertListEqual(result.collect(),
                             [('217.69.143.60', (100, 4000)), ('217.69.143.60', (100, 4000)),
                              ('192.168.30.2', (1500, 54000)), ('192.168.30.2', (200, 3000)),
                              ('192.168.30.2', (200, 3000))], "Lists should be equal")

    def test_seaprate_key_from_end(self):
        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        rdd = sc.parallelize([
            (100, 4000, "217.69.143.60"),
            (100, 4000, "217.69.143.60"),
            (1500, 54000, "192.168.30.2"),
            (200, 3000, "192.168.30.2"),
            (200, 3000, "192.168.30.2")
        ])

        config = Config(CONFIG_PATH)
        aggregation_processor = AggregationProcessor(config, StructType([packet_size, traffic, src_ip]))

        separate_key = aggregation_processor._get_separate_key_lambda()
        result = separate_key(rdd)
        self.assertListEqual(result.collect(),
                             [('217.69.143.60', (100, 4000)), ('217.69.143.60', (100, 4000)),
                              ('192.168.30.2', (1500, 54000)), ('192.168.30.2', (200, 3000)),
                              ('192.168.30.2', (200, 3000))], "Lists should be equal")

    def test_separate_key_from_center(self):
        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        rdd = sc.parallelize([
            (100, "217.69.143.60", 4000),
            (100, "217.69.143.60", 4000),
            (1500, "192.168.30.2", 54000),
            (200, "192.168.30.2", 3000),
            (200, "192.168.30.2", 3000)
        ])

        config = Config(CONFIG_PATH)
        aggregation_processor = AggregationProcessor(config, StructType([packet_size, src_ip, traffic]))

        separate_key = aggregation_processor._get_separate_key_lambda()
        result = separate_key(rdd)
        self.assertListEqual(result.collect(),
                             [('217.69.143.60', (100, 4000)), ('217.69.143.60', (100, 4000)),
                              ('192.168.30.2', (1500, 54000)), ('192.168.30.2', (200, 3000)),
                              ('192.168.30.2', (200, 3000))], "Lists should be equal")

    def test_build_lambda_for_reduce_by_key(self):
        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        rdd = sc.parallelize([
            ("217.69.143.60", 100, 4000),
            ("217.69.143.60", 100, 4000),
            ("192.168.30.2", 1500, 54000),
            ("192.168.30.2", 200, 3000),
            ("192.168.30.2", 200, 3000)
        ])

        config = Config(CONFIG_PATH)
        aggregation_processor = AggregationProcessor(config, data_struct)

        aggregation_lambda = aggregation_processor.get_aggregation_lambda()
        result = aggregation_lambda(rdd)
        self.assertListEqual(result.collect(),
                             [("192.168.30.2", (1900, 60000)),("217.69.143.60", (200, 8000))],
                             "Lists should be equal")
        spark.stop()
