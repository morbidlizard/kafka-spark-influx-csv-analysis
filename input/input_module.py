import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from errors import InputError
from input.executors import BatchExecutor


class InputConfig:
    """
    InputConfig is a class for reading config file to input module.
    """

    def __init__(self, path_to_config):
        """
        Create a new InputConfig. A param path_to_config should be set.
        :param path_to_config:  Relative path to config file.
        """
        self.path = path_to_config
        with open(path_to_config) as cfg:
            self.content = json.load(cfg)


class ReadFactory():
    """
    Main entry point to input module. A ReadFactory create executable for different
    configuration/
    """

    def __init__(self, input_config):
        """
        Create ReadFactory with set config file

        :param input_config: A object of Config class with input options
        """
        self._config = input_config

    def get_executor(self):
        """
        The getExecutor create executor depending input config file
        :return:
        """
        if ("input" in self._config.content.keys()):
            if (self._config.content["input"]["input_type"] == "csv"):
                return ReadCSVFile(self._config.content["input"]["options"]["filename"]).get_batch_executor()
            raise InputError("Error: {} unsuported input format. ReadFactory cannot create Executable".format(
                self._config.content["input"]))
        raise InputError("Error: Some option was miss in config file. ReadFactory cannot create Executable")


class ReadCSVFile(object):
    """
        readCsvFile is a class for reading data from csv file.
    """

    def __init__(self, path_to_file):
        """
        Create ReadCsvFile and read data from csv file.

        :param path_to_file: path to csv file with data
        """
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
        spark = SparkSession.builder.getOrCreate()
        self.rdd = spark.read.csv(path_to_file, data_struct).rdd

    def get_batch_executor(self):
        """
            getExecutable return Executor object
        """
        return BatchExecutor(self.rdd)
