import json
import os
import unittest

import pyspark.sql.types as types

import errors
from config_parsing.transformations_parser import FieldTransformation, SyntaxTree
from config_parsing.transformations_validator import TransformatoinsValidator
from operations.transformation_operations import TransformationOperations


class TransformationsValidatorTestCase(unittest.TestCase):
    def setUp(self):
        with open(os.path.join(os.path.dirname(__file__),
                               os.path.join("..", "data", "config_data_structure.json"))) as cfg:
            data_structure = json.load(cfg)

        self.data_structure = data_structure
        data_structure_list = list(map(lambda x: (x, data_structure[x]), data_structure.keys()))
        data_structure_sorted = sorted(data_structure_list, key=lambda x: x[1]["index"])
        self.data_structure_pyspark = types.StructType(
            list(map(lambda x: types.StructField(x[0], getattr(types, x[1]["type"])()),
                     data_structure_sorted)))

    def test_validate_work_success(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)
        fields = validator.validate(["src_ip", "dst_ip", "packet_size", "sampling_rate"])
        self.assertEqual(fields, types.StructType([
            types.StructField('src_ip', types.StringType()),
            types.StructField('dst_ip', types.StringType()),
            types.StructField('packet_size', types.LongType()),
            types.StructField('sampling_rate', types.LongType())
        ]), 'StructType should be equal')

    def test_validate_raise_field_not_exists_error(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        with self.assertRaises(errors.FieldNotExists):
            validator.validate(["src_ip", "dst_ip", "packet_size", "sample_rate"])

    def test_validate_rename_field(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        fields = validator.validate([FieldTransformation("size", "packet_size"), "dst_ip"])

        self.assertEqual(fields, types.StructType([
            types.StructField('size', types.LongType()),
            types.StructField('dst_ip', types.StringType())
        ]))

    def test_validate_raise_field_not_exists_when_rename_field(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        with self.assertRaises(errors.FieldNotExists):
            validator.validate([FieldTransformation("size", "not_exists_field"), "dst_ip"])

    def test_validate_raise_operation_not_supported_error(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "not_exists_operation"

        with self.assertRaises(errors.OperationNotSupportedError):
            validator.validate([FieldTransformation("size", syntaxtree), "dst_ip"])

    def test_validate_raise_incorrect_arguments_amount_for_operation_error(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "sum"
        syntaxtree.children = ["1", "2", "3"]

        with self.assertRaises(errors.IncorrectArgumentsAmountForOperationError):
            validator.validate([FieldTransformation("sum", syntaxtree), "dst_ip"])

    def test_validate_raise_incorrect_argument_type_for_operation_error(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "mult"
        syntaxtree.children = ["src_ip", "packet_size"]

        with self.assertRaises(errors.IncorrectArgumentTypeForOperationError):
            validator.validate([FieldTransformation("traffic", syntaxtree), "dst_ip"])

    def test_validate_with_correct_one_level_subtree(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "mult"
        syntaxtree.children = ["packet_size", "sampling_rate"]

        fields = validator.validate([FieldTransformation("traffic", syntaxtree), "dst_ip"])

        self.assertEqual(fields, types.StructType([
            types.StructField('traffic', types.LongType()),
            types.StructField('dst_ip', types.StringType())
        ]))

    def test_validate_with_correct_two_level_subtree(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "sum"
        syntaxtree.children = ["sampling_rate", "packet_size"]

        main_syntax_tree = SyntaxTree()
        main_syntax_tree.operation = "mult"
        main_syntax_tree.children = [syntaxtree, "sampling_rate"]

        fields = validator.validate([FieldTransformation("result", main_syntax_tree), "dst_ip"])

        self.assertEqual(fields, types.StructType([
            types.StructField('result', types.LongType()),
            types.StructField('dst_ip', types.StringType())
        ]))

    def test_validate_raise_operation_not_supported_error_for_subtree(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "not_exists_operator"
        syntaxtree.children = ["1", "2"]

        main_syntax_tree = SyntaxTree()
        main_syntax_tree.operation = "mult"
        main_syntax_tree.children = [syntaxtree, "1"]

        with self.assertRaises(errors.OperationNotSupportedError):
            validator.validate([FieldTransformation("result", main_syntax_tree), "dst_ip"])

    def test_validate_function_with_different_arguments_type(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        main_syntax_tree = SyntaxTree()
        main_syntax_tree.operation = "truncate"
        main_syntax_tree.children = ["src_ip", "5"]

        fields = validator.validate([FieldTransformation("result", main_syntax_tree)])

        self.assertEqual(fields, types.StructType([
            types.StructField("result", types.StringType())
        ]))

    def test_validate_raise_error_for_function_with_different_arguments_type(self):
        validator = TransformatoinsValidator(TransformationOperations({
            "country": "./GeoLite2-Country.mmdb",
            "city": "./GeoLite2-City.mmdb",
            "asn": "./GeoLite2-ASN.mmdb"
        }), self.data_structure_pyspark)

        main_syntax_tree = SyntaxTree()
        main_syntax_tree.operation = "truncate"
        main_syntax_tree.children = ["src_ip", "dst_ip"]

        with self.assertRaises(errors.IncorrectArgumentTypeForOperationError):
            validator.validate([FieldTransformation("result", main_syntax_tree)])