from unittest import TestCase

from pyspark.sql.types import *

from config_parsing.aggregations_parser import AggregationsParser
from errors import NotValidAggregationExpression

timestamp = StructField('timestamp', LongType())  # 1
flow_indicator = StructField('FLOW_indicator', StringType())  # 2
agent_address = StructField('agent_address', StringType())  # 3
input_port = StructField('input_port', IntegerType())  # 4
output_port = StructField('output_port', IntegerType())  # 5
src_mac = StructField('src_mac', StringType())  # 6
dst_mac = StructField('dst_mac', StringType())  # 7
ethernet_type = StructField('ethernet_type', StringType())  # 8
in_vlan = StructField('in_vlan', IntegerType())  # 9
out_vlan = StructField('out_vlan', IntegerType())  # 10
src_ip = StructField('src_ip', StringType())  # 11
dst_ip = StructField('dst_ip', StringType())  # 12
ip_protocol = StructField('ip_protocol', StringType())  # 13
ip_tos = StructField('ip_tos', StringType())  # 14
ip_ttl = StructField('ip_ttl', StringType())  # 15
src_port_or_icmp_type = StructField('src_port_or_icmp_type', IntegerType())  # 16
dst_port_or_icmp_code = StructField('dst_port_or_icmp_code', IntegerType())  # 17
tcp_flags = StructField('tcp_flags', StringType())  # 18
packet_size = StructField('packet_size', LongType())  # 19
ip_size = StructField('ip_size', IntegerType())  # 20
sampling_rate = StructField('sampling_rate', IntegerType())  # 21

data_struct = StructType([timestamp, flow_indicator, agent_address, input_port, output_port,
                          src_mac, dst_mac, ethernet_type, in_vlan, out_vlan, src_ip, dst_ip,
                          ip_protocol, ip_tos, ip_ttl, src_port_or_icmp_type, dst_port_or_icmp_code,
                          tcp_flags, packet_size, ip_size, sampling_rate])


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAggregationsParser(TestCase):
    def test_get_parse_expression(self):
        test_input_rule = "key = input_port; Sum(packet_size)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, StructType([input_port,packet_size,]))
        test_expression_token = test_aggregation_config.get_parse_expression()
        self.assertIsInstance(test_expression_token, dict,
                              "Return value of the get_parse_expression method should be instance of dict")
        self.assertEqual(test_expression_token["operation_type"], "reduceByKey",
                         "The dictionary should be contain pair 'operation_type':'reduce'")
        self.assertIsInstance(test_expression_token["rule"], list,
                              "The dictionary should be contain not empty pair 'rule':list of token")
        self.assertGreater(len(test_expression_token["rule"]), 0,
                           "The dictionary should be contain not empty pair 'rule':list of token")

        # test exception to incorrect type,function name or field name
        test_input_rule = "key = field_name1; Count(field_name2); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)

        with self.assertRaisesRegexp(NotValidAggregationExpression, "^Unsupported function\(s\): {'Count'}$"):
            test_expression_token = test_aggregation_config.get_parse_expression()

    def test__field_validation(self):
        config = TestConfig({"processing": {"aggregations": {"operation_type": "",
                                                             "rule": ""}}})
        test_aggregation_config = AggregationsParser(config, data_struct)

        test_parse = test_aggregation_config._field_validation([('Count', 'field_name2')],
                                                               "Count(field_name2):new_field_name2")
        self.assertIsInstance(test_parse, dict,
                              "Return value of the _field_validation method should be instance of dict")
        self.assertDictEqual(test_parse,
                             {'func_name': 'Count', 'input_field': 'field_name2', 'key': False},
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
        test_input_rule = "Min(field_name1);Count(field_name2); Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        test_expression_token = test_aggregation_config._parse_reduce()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing an exception for special characters
        #
        test_input_rule = "Sum(field_name1)#; Min(key); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce()
        self.assertTrue("Find not valid characters" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for other symbols
        #
        test_input_rule = "Sum(field_name1) sdfsdf; Min(key); Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__check_uniq_key_field(self):
        test_input_rule = "Min(field_name1);Count(field_name2); Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        self.assertTrue(test_aggregation_config._check_unique_key_field([{"input_field": "test1", "key": False},
                                                                         {"input_field": "test2", "key": False}]),
                        "Return value should be true if the input list don't contain key fields with true value")
        self.assertTrue(not test_aggregation_config._check_unique_key_field([{"input_field": "test1", "key": True},
                                                                             {"input_field": "test2", "key": False}]),
                        "Return value should be false if the input list contain key fields with true value")

    def test__parse_reduce_by_key(self):
        test_input_rule = "key = field_name1; Count(field_name2); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce_by_key method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing complex key
        #

        test_input_rule = "key = (field_name1,field_name2); Count(field_name3); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_reduce_by_key method should be instance of list")
        self.assertGreater(len(test_expression_token), 0,
                           "Return value of the _pars_reduce method should not be empty")

        #
        # Testing an exception for two or more key field
        #
        test_input_rule = "key = field_name1; key = field_name2; Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Not uniqueness key field" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for missing the key field
        #
        test_input_rule = "Sum(field_name1); Min(key); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("don't contain key field" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for missing comma
        #
        test_input_rule = "Sum(field_name1) key=key_field; Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for missing parenthesis
        #
        test_input_rule = "key=(key_field1,key_field2 ; Sum(field_name1); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("The number of opening and closing parentheses" in context.exception.args[0],
                        "Catch exception, but it differs from test exception {}".format(context.exception.args[0]))

        #
        # Testing an exception for special characters
        #
        test_input_rule = "Sum(field_name1)#; Min(key); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Find not valid characters" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

        #
        # Testing an exception for other symbols
        #
        test_input_rule = "Sum(field_name1) sdfsdf; Min(key); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_reduce_by_key()
        self.assertTrue("Error in the rule" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__parse_expression(self):
        test_input_rule = "key = field_name1;Count(field_name2); Sum(field_nameN)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        test_expression_token = test_aggregation_config._parse_expression()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_expression method should be instance of list")

        test_input_rule = "sum(field_name1); Count(field_name2); Sum(field_nameN)"
        test_input_operation = "reduce"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        test_expression_token = test_aggregation_config._parse_expression()
        self.assertIsInstance(test_expression_token, list,
                              "Return value of the _pars_expression method should be instance of list")

        test_input_rule = "key = field_name1;Count(field_name2); Sum(field_nameN)"
        test_input_operation = "groupBy"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)

        with self.assertRaises(NotValidAggregationExpression) as context:
            test_expression_token = test_aggregation_config._parse_expression()
        self.assertTrue("The operation" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test__types_and_fields_validation_raise_wrong_function_exception(self):
        # test wrong  function name
        test_input_rule = "key = input_port;Sin(in_vlan); Sum(ip_size)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, StructType([input_port,in_vlan,ip_size]))
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression, "^Unsupported function\(s\): {'Sin'}$"):
            test_validation = test_aggregation_config._types_and_field_names_validation()

    def test__types_and_fields_validation_raise_wrong_field_name_exception(self):
        # test wrong field name
        test_input_rule = "key = input_port;Min(in_vlan_bad); Sum(ip_size)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                         "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, data_struct)
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression, "^Unsupported or unused field\(s\): {'in_vlan_bad'}$"):
            test_validation = test_aggregation_config._types_and_field_names_validation()

    def test__types_and_fields_validation_raise_already_aggregated_field_exception(self):
        test_input_rule = "key = src_ip; Max(packet_size); Min(packet_size)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, StructType([src_ip, packet_size]))
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression, "^Aggregate already aggregated field packet_size$"):
            test_validation = test_aggregation_config._types_and_field_names_validation()

    def test__types_and_fields_validation_raise_wrong_field_type_exception(self):
        # test wrong  type of field
        test_input_rule = "key = input_port;Min(dst_mac); Sum(ip_size)"
        test_input_operation = "reduceByKey"
        config = TestConfig({"processing": {"aggregations": {"operation_type": test_input_operation,
                                                             "rule": test_input_rule}}})
        test_aggregation_config = AggregationsParser(config, StructType([input_port,dst_mac,ip_size]))
        test_aggregation_config._expression = test_aggregation_config._parse_expression()

        with self.assertRaisesRegex(NotValidAggregationExpression, "^Incorrect type of field dst_mac for function Min$"):
            test_validation = test_aggregation_config._types_and_field_names_validation()