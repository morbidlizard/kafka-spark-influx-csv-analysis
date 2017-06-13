import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from errors import InputError, KafkaConnectError
from .executors import BatchExecutor, StreamingExecutor
from pyspark.streaming.kafka import KafkaUtils

string_to_int = lambda x: int(x)
string_to_string = lambda x: x
string_to_float = lambda x: float(x)


def type_to_func(type_field):
    if type_field == IntegerType():
        return string_to_int
    if type_field == LongType():
        return string_to_int
    if type_field == StringType():
        return string_to_string
    if type_field == DoubleType():
        return string_to_float
    if type_field == FloatType():
        return string_to_float


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
                return ReadCSVFile(self._config.content).get_batch_executor()
            elif (self._config.content["input"]["input_type"] == "kafka"):
                return KafkaStreaming(self._config).get_streaming_executor()
            raise InputError("Error: {} unsuported input format. ReadFactory cannot create Executable".format(
                self._config.content["input"]))
        raise InputError("Error: Some option was miss in config file. ReadFactory cannot create Executable")


class KafkaStreaming(object):
    def __init__(self, config):
        self._server = config.content["input"]["options"]["server"]
        self._port = config.content["input"]["options"]["port"]
        self._topic = config.content["input"]["options"]["topic"]
        self._batchDuration = config.content["input"]["options"]["batchDuration"]
        self._sep = config.content["input"]["options"]["sep"]
        self._spark = SparkSession.builder.appName("StreamingDataKafka").getOrCreate()
        sc = self._spark.sparkContext
        sc.addFile(config.content["databases"]["country"])
        sc.addFile(config.content["databases"]["city"])
        sc.addFile(config.content["databases"]["asn"])

        kafka_server = self._server + ":" + str(self._port)
        self._ssc = StreamingContext(sc, self._batchDuration)

        list_conversion_function = list((map(lambda x: type_to_func(x.dataType), config.data_structure_pyspark)))
        ranked_pointer = list(enumerate(list_conversion_function))
        functions_list = list(map(lambda x: lambda list_string: x[1](list_string[x[0]]), ranked_pointer))
        function_convert = lambda x: list(map(lambda func: func(x), functions_list))
        try:
            dstream = KafkaUtils.createDirectStream(self._ssc, [self._topic], {"metadata.broker.list": kafka_server})
            self._dstream = dstream.map(lambda x: function_convert(x[1].split(",")))
        except:
            raise KafkaConnectError("Kafka error: Connection refused: server={} port={} topic={}".
                                    format(self._server, self._port, self._topic))

    def get_streaming_executor(self):
        """
            getExecutable return Executor object
        """
        return StreamingExecutor(self._dstream, self._ssc)


class ReadCSVFile(object):
    """
        readCsvFile is a class for reading data from csv file.
    """

    def __init__(self, config):
        """
        Create ReadCsvFile and read data from csv file.

        :param path_to_file: path to csv file with data
        """

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext
        sc.addFile(config["databases"]["country"])
        sc.addFile(config["databases"]["city"])
        sc.addFile(config["databases"]["asn"])

        self.rdd = spark.read.csv(config["input"]["options"]["filename"], config.data_structure_pyspark).rdd

    def get_batch_executor(self):
        """
            getExecutable return Executor object
        """
        return BatchExecutor(self.rdd)
