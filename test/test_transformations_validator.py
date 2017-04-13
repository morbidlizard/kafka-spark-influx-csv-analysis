import unittest
import errors

import pyspark.sql.types as types
from transformations_parser import FieldTransformation, SyntaxTree
from transformations_validator import TransformatoinsValidator


class TransformationsValidatorTestCase(unittest.TestCase):
    def test_validate_work_success(self):
        validator = TransformatoinsValidator()
        fields = validator.validate(["src_ip", "dst_ip", "packet_size", "sampling_rate"])
        self.assertEqual(fields, types.StructType([
            types.StructField('src_ip', types.StringType()),
            types.StructField('dst_ip', types.StringType()),
            types.StructField('packet_size', types.LongType()),
            types.StructField('sampling_rate', types.LongType())
        ]), 'StructType should be equal')

    def test_validate_raise_field_not_exists_error(self):
        validator = TransformatoinsValidator()

        with self.assertRaises(errors.FieldNotExists):
            validator.validate(["src_ip", "dst_ip", "packet_size", "sample_rate"])

    def test_validate_rename_field(self):
        validator = TransformatoinsValidator()
        fields = validator.validate([FieldTransformation("size", "packet_size"), "dst_ip"])

        self.assertEqual(fields, types.StructType([
            types.StructField('size', types.LongType()),
            types.StructField('dst_ip', types.StringType())
        ]))

    def test_validate_raise_field_not_exists_when_rename_field(self):
        validator = TransformatoinsValidator()
        with self.assertRaises(errors.FieldNotExists):
            validator.validate([FieldTransformation("size", "not_exists_field"), "dst_ip"])

    def test_validate_raise_operation_not_supported_error(self):
        validator = TransformatoinsValidator()

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "not_exists_operation"

        with self.assertRaises(errors.OperationNotSupportedError):
            validator.validate([FieldTransformation("size", syntaxtree), "dst_ip"])

    def test_validate_raise_incorrect_arguments_amount_for_operation_error(self):
        validator = TransformatoinsValidator()

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "sum"
        syntaxtree.children = ["1", "2", "3"]

        with self.assertRaises(errors.IncorrectArgumentsAmountForOperationError):
            validator.validate([FieldTransformation("sum", syntaxtree), "dst_ip"])

    def test_validate_raise_incorrect_argument_type_for_operation_error(self):
        validator = TransformatoinsValidator()

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "mult"
        syntaxtree.children = ["src_ip", "packet_size"]

        with self.assertRaises(errors.IncorrectArgumentTypeForOperationError):
            validator.validate([FieldTransformation("traffic", syntaxtree), "dst_ip"])

    def test_validate_with_correct_one_level_subtree(self):
        validator = TransformatoinsValidator()

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "mult"
        syntaxtree.children = ["packet_size", "sampling_rate"]

        fields = validator.validate([FieldTransformation("traffic", syntaxtree), "dst_ip"])

        self.assertEqual(fields, types.StructType([
            types.StructField('traffic', types.LongType()),
            types.StructField('dst_ip', types.StringType())
        ]))

    def test_validate_with_correct_two_level_subtree(self):
        validator = TransformatoinsValidator()

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "sum"
        syntaxtree.children = ["1", "2"]

        main_syntax_tree = SyntaxTree()
        main_syntax_tree.operation = "mult"
        main_syntax_tree.children = [syntaxtree, "1"]

        fields = validator.validate([FieldTransformation("result", main_syntax_tree), "dst_ip"])

        self.assertEqual(fields, types.StructType([
            types.StructField('result', types.LongType()),
            types.StructField('dst_ip', types.StringType())
        ]))

    def test_validate_raise_operation_not_supported_error_for_subtree(self):
        validator = TransformatoinsValidator()

        syntaxtree = SyntaxTree()
        syntaxtree.operation = "not_exists_operator"
        syntaxtree.children = ["1", "2"]

        main_syntax_tree = SyntaxTree()
        main_syntax_tree.operation = "mult"
        main_syntax_tree.children = [syntaxtree, "1"]

        with self.assertRaises(errors.OperationNotSupportedError):
            validator.validate([FieldTransformation("result", main_syntax_tree), "dst_ip"])
