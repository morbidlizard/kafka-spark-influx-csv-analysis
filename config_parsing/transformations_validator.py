import re

import pyspark.sql.types as types

import errors
from .transformations_parser import FieldTransformation


class TransformatoinsValidator:
    def __init__(self, transformation_operations):
        timestamp = types.StructField('timestamp', types.LongType())  # 1
        flow_indicator = types.StructField('FLOW_indicator', types.StringType())  # 2
        agent_address = types.StructField('agent_address', types.StringType())  # 3
        input_port = types.StructField('input_port', types.LongType())  # 4
        output_port = types.StructField('output_port', types.LongType())  # 5
        src_mac = types.StructField('src_mac', types.StringType())  # 6
        dst_mac = types.StructField('dst_mac', types.StringType())  # 7
        ethernet_type = types.StructField('ethernet_type', types.StringType())  # 8
        in_vlan = types.StructField('in_vlan', types.LongType())  # 9
        out_vlan = types.StructField('out_vlan', types.LongType())  # 10
        src_ip = types.StructField('src_ip', types.StringType())  # 11
        dst_ip = types.StructField('dst_ip', types.StringType())  # 12
        ip_protocol = types.StructField('ip_protocol', types.StringType())  # 13
        ip_tos = types.StructField('ip_tos', types.StringType())  # 14
        ip_ttl = types.StructField('ip_ttl', types.StringType())  # 15
        src_port_or_icmp_type = types.StructField('src_port_or_icmp_type', types.LongType())  # 16
        dst_port_or_icmp_code = types.StructField('dst_port_or_icmp_code', types.LongType())  # 17
        tcp_flags = types.StructField('tcp_flags', types.StringType())  # 18
        packet_size = types.StructField('packet_size', types.LongType())  # 19
        ip_size = types.StructField('ip_size', types.LongType())  # 20
        sampling_rate = types.StructField('sampling_rate', types.LongType())  # 21

        self.current_fields = types.StructType([timestamp, flow_indicator, agent_address, input_port, output_port,
                                                src_mac, dst_mac, ethernet_type, in_vlan, out_vlan, src_ip, dst_ip,
                                                ip_protocol, ip_tos, ip_ttl, src_port_or_icmp_type,
                                                dst_port_or_icmp_code,
                                                tcp_flags, packet_size, ip_size, sampling_rate])

        self.transformation_operations = transformation_operations


    def __get_field(self, field):
        try:
            renamed_field = self.current_fields[field]
            return renamed_field
        except KeyError as ex:
            raise errors.FieldNotExists("Field with name {} not exists".format(field))

    def _validate_syntax_tree(self, tree):
        operation = self.transformation_operations.operations_dict.get(tree.operation, None)
        if operation is not None:
            if len(tree.children) == operation["operands"]:
                for index, ch in enumerate(tree.children):
                    actual_type = None
                    if isinstance(ch, str):  # number or field name
                        result = re.search('^(\d+)$', ch)
                        if result is not None:  # it's number
                             actual_type = types.LongType()
                        else:  # it's field
                            renamed_field = self.__get_field(ch)
                            actual_type = renamed_field.dataType
                    else:  # it's other syntax tree, we should extract operation, get type and verify
                        subtree_operation = self.transformation_operations.operations_dict.get(ch.operation, None)
                        if subtree_operation:
                            actual_type = subtree_operation["result"]
                        else:
                            raise errors.OperationNotSupportedError("Operation {} not supported".format(ch.operation))

                        self._validate_syntax_tree(ch)


                    if actual_type != operation["types"][index]:
                        raise errors.IncorrectArgumentTypeForOperationError(
                            "Operand {} expect operands with type {}, but {} has type {}".format(tree.operation,
                                                                                                 operation["types"][index],
                                                                                                 ch,
                                                                                                 actual_type))
                return operation["result"]
            else:
                raise errors.IncorrectArgumentsAmountForOperationError(
                    "Operand {} expect {} operands, but actual {}".format(tree.operation, operation["operands"],
                                                                          len(tree.children)))
        else:
            raise errors.OperationNotSupportedError("Operation {} not supported".format(tree.operation))

    def validate(self, transformations):
        new_fields = []
        for transformation in transformations:
            if isinstance(transformation, FieldTransformation):  # it's transformed name
                if isinstance(transformation.operation, str):  # it's rename
                    renamed_field = self.__get_field(transformation.operation)
                    new_fields.append(types.StructField(transformation.field_name, renamed_field.dataType))
                else:  # is Syntaxtree
                    field_type = self._validate_syntax_tree(transformation.operation)
                    new_fields.append(types.StructField(transformation.field_name, field_type))
            else:  # no transforms
                new_fields.append(self.__get_field(transformation))
        return types.StructType(new_fields)
