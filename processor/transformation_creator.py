from config_parsing.transformations_parser import FieldTransformation, SyntaxTree
from .geo_operations import country, city, aarea

class TransformationCreator:
    def __init__(self, parsed_transformation, geoip_paths):
        self.parsed_transformation = parsed_transformation
        self.mapping = {
            'timestamp': 0,
            'FLOW_indicator': 1,
            'agent_address': 2,
            'input_port': 3,
            'output_port': 4,
            'src_mac': 5,
            'dst_mac': 6,
            'ethernet_type': 7,
            'in_vlan': 8,
            'out_vlan': 9,
            'src_ip': 10,
            'dst_ip': 11,
            'ip_protocol': 12,
            'ip_tos': 13,
            'ip_ttl': 14,
            'src_port_or_icmp_type': 15,
            'dst_port_or_icmp_code': 16,
            'tcp_flags': 17,
            'packet_size': 18,
            'ip_size': 19,
            'sampling_rate': 20,
        }

        self.operations = {
            "country": lambda ip: country(ip, geoip_paths["country"]),
            "city": lambda ip: city(ip, geoip_paths["city"]),
            "aarea": lambda ip: aarea(ip, geoip_paths["asn"]),
            "sum": lambda x, y: x + y,
            "mult": lambda x, y: x * y,
            "div": lambda x, y: x / y,
            "minus": lambda x, y: x - y
        }

    def __generate_params_list(self, children, row):
        args = []
        for ch in children:
            if isinstance(ch, str):
                args.append(row[self.mapping[ch]] if ch in self.mapping.keys() else ch)
            else: # ch has type syntax tree
                operation = self.operations[ch.operation]
                args.append(operation(*self.__generate_params_list(ch.children,row)))
        return args

    def _get_column_value_lambda(self, index):
        return lambda row: row[index]

    def _make_operation_lambda(self, syntax_tree):
        operation = self.operations[syntax_tree.operation]

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
