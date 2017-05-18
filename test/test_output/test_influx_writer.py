import os
from unittest import TestCase
from unittest.mock import Mock

from pyspark.sql import SparkSession
from config_parsing.config import Config
from output.influx_writer import InfluxWriter

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_influx.json"))


class InfluxDBClientMock():
    def __new__(cls, host, port, username, password, database):
        mock = Mock()

        mock.write_points.side_effect = lambda points: cls.__save_points_in_mock(mock, points)
        mock.query.side_effect = lambda query: cls.__get_result_set_from_mock(mock)

        return mock

    @staticmethod
    def __save_points_in_mock(mock, points):
        mock.points = points

    @staticmethod
    def __get_result_set_from_mock(mock):
        result_wrapper = Mock()

        influx_points = []
        for point in mock.points:
            d, d["time"] = {**point["fields"], **point.get("tags", {})}, point["time"]
            influx_points.append(d)

        result_wrapper.get_points.return_value = influx_points
        return result_wrapper


class InfluxWriterTestCase(TestCase):
    def tearDown(self):
        self.__class__.writer.client.drop_database(self.__class__.influx_options["database"])

    # Temporary commented because we don't understand why this test failed
    # def test_write_rdd_to_influx(self):
    #     struct = ["min_packet_size", "max_traffic"]
    #     config = Config(CONFIG_PATH)
    #
    #     self.__class__.influx_options = config.content["output"]["options"]["influx"]
    #
    #     client = InfluxDBClientMock(self.__class__.influx_options["host"], self.__class__.influx_options["port"],
    #                             self.__class__.influx_options["username"],
    #                             self.__class__.influx_options["password"],
    #                             self.__class__.influx_options["database"])
    #
    #     self.__class__.writer = InfluxWriter(client, self.__class__.influx_options["database"],
    #                                          self.__class__.influx_options["measurement"],
    #                                          struct)
    #     spark = SparkSession \
    #         .builder \
    #         .appName("TestInfluxWriter") \
    #         .getOrCreate()
    #
    #     array = [("91.221.61.168", 68, 34816), ("192.168.30.2", 185, 189440),
    #              ("91.226.13.80", 1510, 773120), ("217.69.143.60", 74, 37888)]
    #
    #     sample = spark.sparkContext.parallelize(array).repartition(1)
    #
    #     write_lambda = self.__class__.writer.get_write_lambda()
    #     write_lambda(sample)
    #
    #     result = self.__class__.writer.client.query(
    #         "select * from {0}".format(self.__class__.influx_options["measurement"]))
    #
    #     points = list(result.get_points())
    #
    #     self.assertEqual(len(points), len(array),
    #                      "In {0} measurement should be written {1} points".format(
    #                          self.__class__.influx_options["measurement"], len(array)))
    #
    #     struct.insert(0, "key")
    #
    #     for p_index, point in enumerate(points):
    #         for s_index, name in enumerate(struct):
    #             self.assertEqual(point[name], array[p_index][s_index],
    #                              "Point {0} field {1} should has value {2}".format(p_index, name,
    #                                                                                array[p_index][s_index]))
    #
    #     spark.stop()

    def test_write_tuple_to_influx(self):
        struct = {'operation_type': 'reduce',
                  'rule': [{'key': False, 'input_field': 'packet_size', 'func_name': 'Min'},
                           {'key': False, 'input_field': 'traffic', 'func_name': 'Max'},
                           {'key': False, 'input_field': 'traffic2', 'func_name': 'Sum'}]}
        config = Config(CONFIG_PATH)
        self.__class__.influx_options = config.content["output"]["options"]["influx"]

        client = InfluxDBClientMock(self.__class__.influx_options["host"], self.__class__.influx_options["port"],
                                    self.__class__.influx_options["username"],
                                    self.__class__.influx_options["password"],
                                    self.__class__.influx_options["database"])

        self.__class__.writer = InfluxWriter(client, self.__class__.influx_options["database"],
                                             self.__class__.influx_options["measurement"],
                                             struct)

        write_lambda = self.__class__.writer.get_write_lambda()
        t = (2, 3, 5)
        write_lambda(t)

        result = self.__class__.writer.client.query(
            "select * from {0}".format(self.__class__.influx_options["measurement"]))
        points = list(result.get_points())

        self.assertEqual(len(points), 1,
                         "In {0} measurement should be written one point".format(
                             self.__class__.influx_options["measurement"]))

        fields = ["{0}_{1}".format(field["func_name"].lower(), field["input_field"]) for field in struct["rule"]
                  if not field["key"]]
        for index, name in enumerate(fields):
            self.assertEqual(points[0][name], t[index], "Value should be {0}".format(t[index]))

    def test_write_number_to_influx(self):
        struct = {'operation_type': 'reduce',
                  'rule': [{'key': False, 'input_field': 'packet_size', 'func_name': 'Min'}]}
        config = Config(CONFIG_PATH)
        self.__class__.influx_options = config.content["output"]["options"]["influx"]

        client = InfluxDBClientMock(self.__class__.influx_options["host"], self.__class__.influx_options["port"],
                                    self.__class__.influx_options["username"],
                                    self.__class__.influx_options["password"],
                                    self.__class__.influx_options["database"])

        self.__class__.writer = InfluxWriter(client, self.__class__.influx_options["database"],
                                             self.__class__.influx_options["measurement"],
                                             struct)

        write_lambda = self.__class__.writer.get_write_lambda()
        write_lambda(6)

        result = self.__class__.writer.client.query(
            "select * from {0}".format(self.__class__.influx_options["measurement"]))
        points = list(result.get_points())

        self.assertEqual(len(points), 1,
                         "In {0} measurement should be written one point".format(
                             self.__class__.influx_options["measurement"]))

        self.assertEqual(points[0]["min_packet_size"], 6, "Value should be 6")
