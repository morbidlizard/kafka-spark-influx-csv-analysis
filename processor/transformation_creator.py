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

from config_parsing.transformations_parser import FieldTransformation


class TransformationCreator:
    def __init__(self, data_structure, parsed_transformation, transformation_operations):
        self.parsed_transformation = parsed_transformation
        self.mapping = dict(map(lambda x: (x, data_structure[x]["index"]), data_structure.keys()))
        self.transformation_operations = transformation_operations

    def __generate_params_list(self, children, row):
        args = []
        for ch in children:
            if isinstance(ch, str):
                args.append(row[self.mapping[ch]] if ch in self.mapping.keys() else int(ch))
            else: # ch has type syntax tree
                operation = self.transformation_operations.operations_dict[ch.operation]["lambda"]
                args.append(operation(*self.__generate_params_list(ch.children,row)))
        return args

    def _get_column_value_lambda(self, index):
        return lambda row: row[index]

    def _make_operation_lambda(self, syntax_tree):
        operation = self.transformation_operations.operations_dict[syntax_tree.operation]["lambda"]

        return lambda row: operation(*self.__generate_params_list(syntax_tree.children, row))

    def build_lambda(self):
        lambdas = []
        for exp_tr in self.parsed_transformation:
            if isinstance(exp_tr, FieldTransformation):
                if isinstance(exp_tr.operation, str):
                    lambdas.append(self._get_column_value_lambda(self.mapping[exp_tr.operation]))
                else:
                    syntax_tree = exp_tr.operation
                    lambdas.append(self._make_operation_lambda(syntax_tree))
            else:
                lambdas.append(self._get_column_value_lambda(self.mapping[exp_tr]))

        return lambda row: (tuple(map(lambda x: x(row), lambdas)))
