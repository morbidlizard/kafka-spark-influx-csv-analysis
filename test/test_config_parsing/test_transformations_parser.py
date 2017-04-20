import os
import unittest

import errors
from config_parsing.transformations_parser import TransformationsParser, TransformationsParserConfig, SyntaxTree, \
    FieldTransformation

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config.json"))

stub = {
    "sum": ["2", "2", "3"],
    "first_mult": ["1", "3"],
    "second_mult": [
        "1",
        ["2", "3"]
    ],
    "run_test": [
        {  # 0
            "type": str,
            "field_name": "source_ip"
        },
        {  # 1
            #skip
        },
        {  # 2
            "type": SyntaxTree,
            "field_name": "src_country"
        },
        {  # 3
            "type": SyntaxTree,
            "field_name": "traffic"
        }
    ]
}


class TransformationsParserTest(unittest.TestCase):
    def test__parse_field(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(config)

        result = parser._parse("sample_rating")
        self.assertIsInstance(result, str, "Result should be instance of string")
        self.assertEqual(result, "sample_rating", "Value this leaf node should be 'sample_rating'")

    def test__parse_simple_operation(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(config)

        expression = "sum({})".format(",".join(stub["sum"]))

        result = parser._parse(expression)
        self.assertIsInstance(result, SyntaxTree, "Result should be instance of SyntaxTree")
        self.assertEqual(result.operation, "sum", "Operation should be 'sum'")
        self.assertEqual(len(result.children), 3, "Should have 3 children")

        for index in range(0, 3):
            self.assertIsInstance(result.children[index], str,
                                  "children[{}] should be instance of Leaf".format(index))
            self.assertEqual(result.children[index], stub["sum"][index],
                             "Sum {} argument should be {}".format(index, stub["sum"][index]))

    def test__parse_nested_operations(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(config)

        expression = "minus(mult({}),mult({},sum({})))".format(",".join(stub["first_mult"]), stub["second_mult"][0],
                                                               ",".join(stub["second_mult"][1]))
        result = parser._parse(expression)
        self.assertIsInstance(result, SyntaxTree, "Result should be instance of SyntaxTree")
        self.assertEqual(result.operation, "minus", "Operation should be 'minus'")
        self.assertEqual(len(result.children), 2, "Should have 2 children")

        # Check first child # mult(1,3)
        first_mult = result.children[0]  # mult(1,3)

        self.assertIsInstance(first_mult, SyntaxTree, "Result should be instance of SyntaxTree")
        self.assertEqual(first_mult.operation, "mult", "Operation should be 'mult'")
        self.assertEqual(len(first_mult.children), 2, "Should have 2 children")

        for index in range(0, 2):
            self.assertIsInstance(first_mult.children[index], str,
                                  "children[{}] should be instance of str".format(index))
            self.assertEqual(first_mult.children[index], stub["first_mult"][index],
                             "Mult {} argument should be {}".format(index, stub["first_mult"][index]))

        # Check second child mult(1,sum(2,3))
        second_mult = result.children[1]
        self.assertIsInstance(second_mult, SyntaxTree, "Result should be instance of SyntaxTree")
        self.assertEqual(second_mult.operation, "mult", "Operation should be 'mult'")
        self.assertEqual(len(second_mult.children), 2, "Should have 2 children")

        # second_mult[0] should be 1
        self.assertIsInstance(second_mult.children[0], str,
                              "children[{}] should be instance of str".format(0))
        self.assertEqual(second_mult.children[0], stub["second_mult"][0],
                         "Mult {} argument should be {}".format(0, stub["second_mult"][0]))

        # second_mult[1] should be SyntaxTree
        sub_sum = second_mult.children[1]
        self.assertIsInstance(sub_sum, SyntaxTree,
                              "children[{}] should be instance of SyntaxTree".format(1))
        self.assertEqual(sub_sum.operation, "sum", "Operation should be 'sum'")
        self.assertEqual(len(sub_sum.children), 2, "Should have 2 children")

        for index in range(0, 2):
            self.assertIsInstance(sub_sum.children[index], str,
                                  "children[{}] should be instance of str".format(index))
            self.assertEqual(sub_sum.children[index], stub["second_mult"][1][index],
                             "Sum {} argument should be {}".format(index, stub["second_mult"][1][index]))

    def test__parse_raise_incorrect_expression_error(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(config)

        with self.assertRaises(errors.IncorrectExpression):
            parser._parse("sum((1,2)")

    def test_run(self):
        config = TransformationsParserConfig(CONFIG_PATH)
        parser = TransformationsParser(config)

        parser.run()

        self.assertEqual(len(parser.expanded_transformation), 4, "Transformations should contain 4 elements")
        self.assertEqual(parser.expanded_transformation[1], "dst_ip",
                         "2 element in expanded transformation should be 'dst_ip'")

        for index in [0, 2, 3]:
            self.assertIsInstance(parser.expanded_transformation[index], FieldTransformation,
                                  "{} element expanded transformation should has FieldTransformation type".format(index))

            self.assertEqual(parser.expanded_transformation[index].field_name, stub["run_test"][index]["field_name"],
                             'expanded_transformation[{}].field_name should be {}'.format(index, stub["run_test"][
                                 index]["field_name"]))

            self.assertIsInstance(parser.expanded_transformation[index].operation, stub["run_test"][index]["type"],
                                  'expanded_transformation[{}].operation should be instance of {}'.format(index, stub[
                                      "run_test"][index]["type"]))