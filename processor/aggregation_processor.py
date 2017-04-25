from config_parsing.aggregations_parser import SupportedReduceOperations, AggregationsParser


class AggregationProcessor:
    def __init__(self, config_processor, input_data_structure):
        self.config_processor = config_processor
        support_reduce_operations = SupportedReduceOperations()
        self._support_reduce_operations = support_reduce_operations.operation
        self._input_data_structure = input_data_structure

        self._input_field_name = list(map(lambda x: x.name, self._input_data_structure))
        aggregation_expression = AggregationsParser(config_processor, self._input_data_structure)
        self._aggregation_expression = aggregation_expression.get_parse_expression()

        self._field_to_func_name = {(field["input_field"]): (field["func_name"]) for field in
                                    self._aggregation_expression["rule"] if not field["key"]}

    def get_aggregation_lambda(self):

        ordered_pointers_to_function = [self._support_reduce_operations[self._field_to_func_name[exp_tr]]
                                        ["ref_to_func"] for exp_tr in self._input_field_name]
        ranked_pointer = list(enumerate(ordered_pointers_to_function))
        functions_list = list(map(lambda x: lambda row1, row2: x[1](row1[x[0]], row2[x[0]]), ranked_pointer))

        return lambda row1, row2: (tuple(map(lambda x: x(row1, row2), functions_list)))

