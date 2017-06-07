import json
import os
import types
from pyspark.sql import types as types_spark
from unittest import TestCase

from pyspark.sql import SparkSession

from config_parsing.transformations_parser import FieldTransformation, SyntaxTree
from operations.transformation_operations import TransformationOperations
from processor.transformation_creator import TransformationCreator

DATA_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "test.csv"))


class TransformationCreatorTestCase(TestCase):
    def setUp(self):
        with open(os.path.join(os.path.dirname(__file__),
                               os.path.join("..", "data", "config_data_structure.json"))) as cfg:
            data_structure = json.load(cfg)

        self.data_structure = data_structure
        data_structure_list = list(map(lambda x: (x, data_structure[x]), data_structure.keys()))
        data_structure_sorted = sorted(data_structure_list, key=lambda x: x[1]["index"])
        self.data_structure_pyspark = types_spark.StructType(
            list(map(lambda x: types_spark.StructField(x[0], getattr(types_spark, x[1]["type"])()),
                     data_structure_sorted)))

    def test_build_lambda(self):
        mult_syntax_tree = SyntaxTree()
        mult_syntax_tree.operation = "mult"
        mult_syntax_tree.children = ["packet_size", "sampling_rate"]

        parsed_transformations = ["src_ip", FieldTransformation("destination_ip", "dst_ip"),
                                  FieldTransformation("traffic", mult_syntax_tree)]

        creator = TransformationCreator(self.data_structure, parsed_transformations, TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType, "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(result, [("217.69.143.60", "91.221.61.183", 37888),
                                      ("91.221.61.168", "90.188.114.141", 34816),
                                      ("91.226.13.80", "5.136.78.36", 773120),
                                      ("192.168.30.2", "192.168.30.1", 94720),
                                      ("192.168.30.2", "192.168.30.1", 94720)], "List of tuples should be equal")

        spark.stop()

    def test_build_lambda_with_nested_operations(self):
        mult_syntax_tree = SyntaxTree()
        mult_syntax_tree.operation = "mult"
        mult_syntax_tree.children = ["packet_size", "sampling_rate"]

        root_mult_st = SyntaxTree()
        root_mult_st.operation = "mult"
        root_mult_st.children = [mult_syntax_tree, "sampling_rate"]

        parsed_transformations = ["src_ip", FieldTransformation("destination_ip", "dst_ip"),
                                  FieldTransformation("traffic", root_mult_st)]

        creator = TransformationCreator(self.data_structure, parsed_transformations, TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }))

        transformation = creator.build_lambda()

        self.assertIsInstance(transformation, types.LambdaType, "Transformation type should be lambda")

        spark = SparkSession.builder.getOrCreate()
        file = spark.read.csv(DATA_PATH, self.data_structure_pyspark)

        result = file.rdd.map(transformation)

        result = result.collect()

        self.assertListEqual(result, [("217.69.143.60", "91.221.61.183", 37888 * 512),
                                      ("91.221.61.168", "90.188.114.141", 34816 * 512),
                                      ("91.226.13.80", "5.136.78.36", 773120 * 512),
                                      ("192.168.30.2", "192.168.30.1", 94720 * 512),
                                      ("192.168.30.2", "192.168.30.1", 94720 * 512)],
                             "List of tuples should be equal")

        spark.stop()
