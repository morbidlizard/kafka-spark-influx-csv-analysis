import re
from errors import NotValidAggregationExpression
from pyspark.sql.types import *

Sum = lambda x, y: x + y
Mult = lambda x, y: x * y
Min = lambda x, y: x if x < y else y
Max = lambda x, y: x if x > y else y


class SupportedReduceOperations:
    def __init__(self):
        self.operation = {"Sum": {"ref_to_func": Sum, "input_type": LongType(), "output_type": LongType()},
                          "Mult": {"ref_to_func": Mult, "input_type": LongType(), "output_type": LongType()},
                          "Min": {"ref_to_func": Min, "input_type": LongType(), "output_type": LongType()},
                          "Max": {"ref_to_func": Max, "input_type": LongType(), "output_type": LongType()}}
        self._type_transform_rule = {DoubleType(): 5, FloatType(): 4, LongType(): 3, IntegerType(): 2,
                                     ShortType(): 1, ByteType(): 0}
        self.numeric_types = self._type_transform_rule.keys()

    def check_type_arg_function(self, type_input_arg, function_name):
        if (self._type_transform_rule[self.operation[function_name]["input_type"]] >= self._type_transform_rule[
            type_input_arg]) \
                and (self._type_transform_rule[type_input_arg] >= self._type_transform_rule[
                    self.operation[function_name]["output_type"]]):
            return True
        else:
            return False


class AggregationsParser:
    def __init__(self, config, input_data_structure):
        self._config = config.content["processing"]["aggregations"]
        self._input_rule = config.content["processing"]["aggregations"]["rule"]
        self._regexp_reducefield = '\s*(\w+)\((\s*\w+\s*)\)\s*'
        self._regexp_keyfield = '\s*(key)\s*\=(\s*\(*[\w\\,\s]*\s*\)*)\s*'
        self._input_data_structure = input_data_structure
        self._expression = []

    def _types_and_field_names_validation(self):
        """
        The method validates parse expression on types, fields name and function names
        :return: return true if list of dictionaries is valid and false at other case
        """
        # check function name
        reduce_operation = SupportedReduceOperations()
        set_expression_functions = set(x['func_name'] for x in self._expression if not x['key'])
        set_support_functions = set(reduce_operation.operation.keys())
        set_expression_fields = set(map(lambda x: x['input_field'], self._expression))
        set_input_fields_data_structure = set(map(lambda x: x.name, self._input_data_structure))
        dict_input_field_type = dict(map(lambda x: [x.name, x.dataType], self._input_data_structure))
        # check unsupported function

        unsupported_functions = set_expression_functions - set_support_functions
        if unsupported_functions:
            raise NotValidAggregationExpression("Unsupported function(s): {}".format(unsupported_functions))

        if set_expression_fields != set_input_fields_data_structure:
            raise NotValidAggregationExpression(
                "Unsupported or unused field(s): {}".format(set_expression_fields - set_input_fields_data_structure))

        already_aggregated = []
        for field in self._expression:
            if not field['key']:
                # We should raise error if field already aggregated
                if field["input_field"] not in already_aggregated:
                    already_aggregated.append(field["input_field"])
                else:
                    raise NotValidAggregationExpression("Aggregate already aggregated field {}".format(
                        field["input_field"]))

                if dict_input_field_type[field['input_field']] in reduce_operation.numeric_types:
                    if not reduce_operation.check_type_arg_function(dict_input_field_type[field['input_field']],
                                                                    field['func_name']):
                        raise NotValidAggregationExpression(
                            "Incorrect type of field {} for function {}".format(field['input_field'],
                                                                                field['func_name']))
                else:
                    if dict_input_field_type[field['input_field']] != \
                            reduce_operation.operation[field['func_name']]['input_type']:
                        raise NotValidAggregationExpression(
                            "Incorrect type of field {} for function {}".format(field['input_field'],
                                                                                field['func_name']))

    def get_parse_expression(self):
        """
        The public method return valid list of token dictionary
        :return: return name of operation ("operation_type") and valid list of dictionaries ("rule"). Every dictionary
                include next field: function = field with function, input_field = input field name from source data.
        """
        self._expression = self._parse_expression()
        self._types_and_field_names_validation()

        return {"operation_type": self._config["operation_type"], "rule": self._expression}

    def _field_validation(self, re_match_list, field):
        """
        The method validates the field on correct syntax

        :param re_match_list: list of find match in field
        :param field: field that parse
        :return: dictionary include next field: function = field with function,
            input_field = input field name from source data.
        """
        expression = {}
        number_match = len(re_match_list)
        if number_match == 1:
            function_name = re_match_list[0][0]
            input_field = re_match_list[0][1]
            expression["func_name"] = function_name
            expression["input_field"] = input_field
            expression["key"] = False
        elif number_match == 0:
            raise NotValidAggregationExpression("Error: Error in the field %s. Perhaps a parenthesis is "
                                                "missing or semicolon" % field)
        else:
            raise NotValidAggregationExpression("Error: Error in the rule %s. Perhaps a semicolon is missing."
                                                % self._input_rule)
        return expression

    def _parse_reduce(self):
        """
        The method parse the rule of reduce operation and raise the exception if not valid the syntax of input string
        :return: return list of dictionaries. Every dictionary include next field: function = field with function,
            input_field = input field name from source data.
        """
        separate_fields = self._input_rule.split(";")

        output_list = []
        for field in separate_fields:
            if self._check_field_on_valid_characters(field):
                residue_field = re.sub(self._regexp_reducefield, '', field)
                residue_field = re.sub(self._regexp_keyfield, '', residue_field)
                residue_field = re.sub('\s+', '', residue_field)
                if len(residue_field) == 0:
                    re_match_list = re.findall(self._regexp_reducefield, field)
                    output_list.append(self._field_validation(re_match_list, field))
                else:
                    raise NotValidAggregationExpression(
                        "Error: Error in the rule '%s'. Perhaps a semicolon is missing." %
                        self._input_rule)
            else:
                raise NotValidAggregationExpression("Error: Error in the field '%s'. Find not valid characters" %
                                                    field)
        return output_list

    def _check_unique_key_field(self, list_field):
        """
        The method checks uniqueness of key field
        :param list_field: A list of parse expression
        :return: true if name field uniqueness else false
        """
        return not [field["input_field"] for field in list_field if field["key"]]

    def _check_field_on_valid_characters(self, field):
        """
        The method check field on correct character.
        :param field: input field
        :return: true if field contain valid character and false at other case
        """
        return not len(re.sub('[a-zA-Z0-9\(\)\_\s\=\,]', '', field)) > 0

    def _parse_reduce_by_key(self):
        """
        The function parse the rule of reduce operation and raise exception if not valid the syntax of input string
        :return: return list of dictionaries. Every dictionary include next field: function = field with function,
            input_field = input field name from source data.
        """
        separate_fields = self._input_rule.split(";")

        output_list = []
        for field in separate_fields:
            if self._check_field_on_valid_characters(field):
                re_match_list = re.findall(self._regexp_reducefield, field)
                re_match_key_field = re.findall(self._regexp_keyfield, field)
                residue_field = re.sub(self._regexp_reducefield, '', field)
                residue_field = re.sub(self._regexp_keyfield, '', residue_field)
                residue_field = re.sub('\s+', '', residue_field)
                if len(residue_field) == 0:
                    if re_match_key_field and not re_match_list and len(re_match_key_field):

                        list_expression = []
                        key_field = re_match_key_field[0][1]
                        if self._check_unique_key_field(output_list):
                            if (key_field.count("(") == key_field.count(")")):
                                key_field = key_field.replace("(", "")
                                key_field = key_field.replace(")", "")
                                key_field = re.sub('\s+', '', key_field)
                                list_key_field = key_field.split(",")
                                for field in list_key_field:
                                    expression = {}
                                    expression["func_name"] = ""
                                    expression["input_field"] = field
                                    expression["key"] = True
                                    list_expression.append(expression)
                            else:
                                raise NotValidAggregationExpression(
                                    "Error: The number of opening and closing parentheses does not match '%s'." %
                                    key_field)
                            if output_list:
                                output_list = list_expression + (output_list)
                            else:
                                output_list = list_expression
                        else:
                            raise NotValidAggregationExpression("Error: Not uniqueness key field in rule '%s'." %
                                                                self._input_rule)
                    elif not re_match_key_field and re_match_list:
                        expression = self._field_validation(re_match_list, field)
                        output_list.append(expression)
                    else:
                        raise NotValidAggregationExpression(
                            "Error: Error in the rule '%s'. Perhaps a semicolon is missing." %
                            self._input_rule)
                else:
                    raise NotValidAggregationExpression(
                        "Error: Error in the rule '%s'. Perhaps a semicolon is missing." %
                        self._input_rule)
            else:
                raise NotValidAggregationExpression("Error: Error in the field '%s'. Find not valid characters" %
                                                    field)
        if not self._check_unique_key_field(output_list):
            return output_list
        else:
            raise NotValidAggregationExpression("Error: The rule '%s' don't contain key field." % self._input_rule)

    def _parse_expression(self):
        """
        The method parse expression with according operation type.
        :return: return list of dictionaries. Every dictionary include next field: function = field with function,
            input_field = input field name from source data.
        """
        operation = self._config["operation_type"]
        if operation == "reduce":
            return self._parse_reduce()
        elif operation == "reduceByKey":
            return self._parse_reduce_by_key()
        else:
            raise NotValidAggregationExpression("The operation '%s' don't support " % operation)
