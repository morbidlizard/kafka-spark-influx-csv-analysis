from unittest import TestCase

from errors import NotValidAggregationExpression
from aggregations_parser import AggregationsParser


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAggregationsParser(TestCase):
    def test_get_parse_expression(self):
        test_input_rule = "key = field_name1,Count(field_name2), Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        test_expression_token = test_aggregation_config.get_parse_expression()
        self.assertIsInstance(test_expression_token, dict,
                              "Return value of the get_parse_expression method should be instance of dict")
        self.assertEqual(test_expression_token["operation_type"], "reduceByKey",
                         "The dictionary should be contain pair 'operation_type':'reduce'")
        self.assertIsInstance(test_expression_token["rule"], list,
                              "The dictionary should be contain not empty pair 'rule':list of token")
        self.assertGreater(len(test_expression_token["rule"]), 0,
                           "The dictionary should be contain not empty pair 'rule':list of token")

    def test__field_validation(self):
        config = TestConfig({"processing": {"aggregations": {"operation_type": "",
                                                             "rule": ""}}})
        test_aggregation_config = AggregationsParser(config)

        test_parse = test_aggregation_config._field_validation([('Count', 'field_name2')],
                                                               "Count(field_name2):new_field_name2")
        self.assertIsInstance(test_parse, dict,
                              "Return value of the _field_validation method should be instance of dict")
        self.assertDictEqual(test_parse,
                             {'func_name': 'Count', 'input_field': 'field_name2'},
                             "Dictionary should contain next pair:func_name: value, input_field: "
                             "value")

        # test exception when find 2 and more regexp in field
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_parse = test_aggregation_config._field_validation([('Count', 'field_name2'), ('Sum', 'field_name3')],
                                                                   "Count(field_name2):new_field_name2")
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        # test exception when don't find regexp in field
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_parse = test_aggregation_config._field_validation([], "")
        self.assertTrue("Error in the field" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__pars_reduce(self):
        test_input_rule = "Min(field_name1),Count(field_name2), Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        test_expression_token = test_aggregation_config._parse_reduce()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing an exception for special characters
        #
        test_input_rule = "Sum(field_name1)#, Min(key), Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce()
        self.assertTrue("Find not valid characters" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for other symbols
        #
        test_input_rule = "Sum(field_name1) sdfsdf, Min(key), Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")


    def test__check_uniq_key_field(self):
        test_input_rule = "Min(field_name1),Count(field_name2), Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        self.assertTrue(test_aggregation_config._check_unique_key_field([{"input_field": "test1", "key": False},
                                                                         {"input_field": "test2", "key": False}]),
                        "Return value should be true if the input list don't contain key fields with true value")
        self.assertTrue(not test_aggregation_config._check_unique_key_field([{"input_field": "test1", "key": True},
                                                                             {"input_field": "test2", "key": False}]),
                        "Return value should be false if the input list contain key fields with true value")

    def test__pars_reduce_by_key(self):
        test_input_rule = "key = field_name1, Count(field_name2), Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce_by_key method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing an exception for two or more key field
        #
        test_input_rule = "key = field_name1, key = field_name2, Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Not uniqueness key field" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for missing the key field
        #
        test_input_rule = "Sum(field_name1), Min(key), Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("don't contain key field" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for missing comma
        #
        test_input_rule = "Sum(field_name1) key=key_field, Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for special characters
        #
        test_input_rule = "Sum(field_name1)#, Min(key), Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Find not valid characters" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for other symbols
        #
        test_input_rule = "Sum(field_name1) sdfsdf, Min(key), Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__pars_expression(self):

        test_input_rule = "key = field_name1,Count(field_name2), Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        test_expression_token = test_aggregation_config._parse_expression()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_expression method should be instance of list")

        test_input_rule = "sum(field_name1), Count(field_name2), Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)
        test_expression_token = test_aggregation_config._parse_expression()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_expression method should be instance of list")

        test_input_rule = "key = field_name1,Count(field_name2), Sum(field_nameN)"
        test_input_operation = "groupBy"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config)

        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_expression()
        self.assertTrue("The operation" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")
