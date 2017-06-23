# Copyright 2017, bwsoft management
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
from config_parsing.aggregations_parser import AggregationsParser
from operations.aggregation_operations import SupportedReduceOperations


class AggregationProcessor:
    def __init__(self, config_processor, input_data_structure):
        self.config_processor = config_processor
        self._input_data_structure = input_data_structure

        self._support_reduce_operations = SupportedReduceOperations().operation
        self.key_data = []

        aggregation_expression = AggregationsParser(config_processor, self._input_data_structure)
        self._aggregation_expression = aggregation_expression.get_parse_expression()

        self._input_field_name = [struct_field.name for struct_field in self._input_data_structure]

        aggregation_data = copy.deepcopy(self._aggregation_expression)
        if self._aggregation_expression["operation_type"] == "reduceByKey":
            key_struct_list = [token for token in self._aggregation_expression["rule"] if token["key"]]
            # (key_index,key_struct_field)
            for key_struct in key_struct_list:
                self.key_data.append((self._input_field_name.index(key_struct["input_field"]), key_struct))
            for key_struct in key_struct_list:
                aggregation_data["rule"].remove(key_struct)
                self._input_field_name.remove(key_struct["input_field"])

        self._field_to_func_name = {(field["input_field"]): (field["func_name"]) for field in
                                    aggregation_data["rule"]}
        self._enumerate_output_field = dict(map(lambda x: (x[1], x[0]), enumerate(self._input_field_name)))

    def get_enumerate_field(self):
        return self._enumerate_output_field

    def get_output_structure(self):
        return self._aggregation_expression

    # input row: (field_1,..,key,..field_n) -> (key, (field_1,..field_n))
    def _build_separate_key_lambda(self):
        num_field = [self._input_data_structure.names.index(field) for field in self._input_field_name]
        lambdas_key = list(map(lambda x: lambda row: row[x[0]], self.key_data))
        lambdas_field = list(map(lambda x: lambda row: row[x], num_field))
        return lambda row: (tuple(map(lambda x: x(row), lambdas_key)),
                            tuple(map(lambda x: x(row), lambdas_field)))

    # input row: (key, (field_1,..field_n)) -> (key,field_1,..,field_n)
    def _bulid_postprocessing_lambda(self):
        postprocessing = lambda row: tuple([row[0]] + list(row[1]))
        return lambda rdd: rdd.map(postprocessing)

    # apply separate key lambda to rdd
    def _get_separate_key_lambda(self):
        separate_key_lambda = self._build_separate_key_lambda()
        return lambda rdd: rdd.map(separate_key_lambda)

    # apply aggregation to rdd
    def _make_reduce_by_key_aggregation(self):
        aggregation = self.build_aggregation_lambda()
        return lambda rdd: rdd.reduceByKey(aggregation)

    def build_aggregation_lambda(self):
        ordered_pointers_to_function = [self._support_reduce_operations[self._field_to_func_name[exp_tr]]
                                        ["ref_to_func"] for exp_tr in self._input_field_name]

        # ranked pointers contains pairs (field_number , function)
        ranked_pointer = list(enumerate(ordered_pointers_to_function))
        # x - function, row1 and row2 - two different strings, we get necessary fields as arguments to function
        functions_list = list(map(lambda x: lambda row1, row2: x[1](row1[x[0]], row2[x[0]]), ranked_pointer))

        return lambda row1, row2: (tuple(map(lambda x: x(row1, row2), functions_list)))

    def get_aggregation_lambda(self):
        if self.key_data:
            separator = self._get_separate_key_lambda()
            aggregation = self._make_reduce_by_key_aggregation()
            postprocessing = self._bulid_postprocessing_lambda()
            return lambda rdd: postprocessing(aggregation(separator(rdd)))

        aggregation = self.build_aggregation_lambda()
        return lambda rdd: rdd.reduce(aggregation) if not rdd.isEmpty() else rdd
        # return lambda rdd: rdd.reduce(aggregation)
