from unittest import TestCase
import types
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType
from processor.aggregation_processor import AggregationProcessor


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAggregationProcessor(TestCase):

    def test_build_lambda(self):
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

        test_result = test_rdd.reduce(test_aggregation_lambda)
        self.assertTupleEqual(test_result, (7, 0, 5), "Error in aggregation operation. Tuple should be equal")
