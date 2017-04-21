import types
import os

from unittest import TestCase
from pyspark.sql import SparkSession

from processor.transformation_creator import TransformationCreator
from config_parsing.transformations_parser import FieldTransformation, SyntaxTree

DATA_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "test.csv"))


class TransformationCreatorTestCase(TestCase):
    def test_build_lambda(self):
        mult_syntax_tree = SyntaxTree()
        mult_syntax_tree.operation = "mult"
        mult_syntax_tree.children = ["packet_size", "sampling_rate"]

        country_of_syntax_tree = SyntaxTree()
        country_of_syntax_tree.operation = "country_of"
        country_of_syntax_tree.children = ["src_ip"]

        parsed_transformations = ["src_ip", FieldTransformation("destination_ip", "dst_ip"),
                                  FieldTransformation("src_country", country_of_syntax_tree),
                                  FieldTransformation("traffic", mult_syntax_tree)]

        creator = TransformationCreator(parsed_transformations)
        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType, "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(result, [("217.69.143.60", "91.221.61.183", 'USA', 37888),
                                      ("91.221.61.168", "90.188.114.141", 'USA', 34816),
                                      ("91.226.13.80", "5.136.78.36", 'USA', 773120),
                                      ("192.168.30.2", "192.168.30.1", 'USA', 94720),
                                      ("192.168.30.2", "192.168.30.1", 'USA', 94720)], "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_with_nested_operations(self):
        mult_syntax_tree = SyntaxTree()
        mult_syntax_tree.operation = "mult"
        mult_syntax_tree.children = ["packet_size", "sampling_rate"]

        root_mult_st = SyntaxTree()
        root_mult_st.operation = "mult"
        root_mult_st.children = [mult_syntax_tree, "10"]

        country_of_syntax_tree = SyntaxTree()
        country_of_syntax_tree.operation = "country_of"
        country_of_syntax_tree.children = ["src_ip"]

        parsed_transformations = ["src_ip", FieldTransformation("destination_ip", "dst_ip"),
                                  FieldTransformation("src_country", country_of_syntax_tree),
                                  FieldTransformation("traffic", root_mult_st)]

        creator = TransformationCreator(parsed_transformations)
        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType, "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(result, [("217.69.143.60", "91.221.61.183", 'USA', 378880),
                                      ("91.221.61.168", "90.188.114.141", 'USA', 348160),
                                      ("91.226.13.80", "5.136.78.36", 'USA', 7731200),
                                      ("192.168.30.2", "192.168.30.1", 'USA', 947200),
                                      ("192.168.30.2", "192.168.30.1", 'USA', 947200)],
                             "List of tuples should be equal")

        spark.stop()
