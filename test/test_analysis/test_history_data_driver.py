import re
import unittest
from unittest.mock import Mock
from datetime import datetime
from analysis.history_data_driver import HistoryDataDriver


class InfluxDBClientMock():
    def __new__(cls, host, port, username, password, database):
        mock = Mock()

        mock.write_points.side_effect = lambda points: cls.__save_points_in_mock(mock, points)
        mock.query.side_effect = lambda query: cls.__get_result_set_from_mock(mock, query)

        return mock

    @staticmethod
    def __save_points_in_mock(mock, points):
        mock.points = points

    @staticmethod
    def __get_result_set_from_mock(mock, query):
        result_wrapper = Mock()
        influx_points = []

        result = re.search(r"(\d{19}).*?(\d{19})", query)
        start, end = result.groups()

        # extract range
        points = list(filter(lambda point: point["time"] > int(start) and point["time"] < int(end),
                             mock.points))

        country_result = re.search(r"\"country\"=\'(\w+)\'", query)
        if country_result:
            country = country_result.groups()[0]
            points = list(filter(lambda point: point["tags"]["country"] == country, points))

        for point in points:
            d = {**point["fields"], **point.get("tags", {})}
            d["time"] = datetime.utcfromtimestamp(point["time"] // 1000000000).strftime('%Y-%m-%dT%H:%M:%SZ')
            influx_points.append(d)

        result_wrapper.get_points.return_value = influx_points
        return result_wrapper


class HistoryDataDriverTestCase(unittest.TestCase):
    client = InfluxDBClientMock("localhost", 8086, "root", "root", "test")

    def setUp(self):
        __class__.client.create_database("test")

        countries = ["Russia", "USA"]
        timestamps = [1495005255000000000, 1495005256000000000, 1495005257000000000, 1495005258000000000]
        points = [{"measurement": "points", "tags": {"country": countries[index % 2]}, "fields": {"sum_traffic": 12345},
                   "time": timestamp} for index, timestamp in enumerate(timestamps)]

        __class__.client.write_points(points)

    def tearDown(self):
        __class__.client.drop_database("test")

    def test_read_without_tags(self):
        history_data_driver = HistoryDataDriver(__class__.client)
        result = history_data_driver.read("points", 1495005255000000000, 1495005258000000000)

        self.assertListEqual(result, [{"sum_traffic": 12345, "country": "USA", "time": "2017-05-17T07:14:16Z"},
                                      {"sum_traffic": 12345, "country": "Russia", "time": "2017-05-17T07:14:17Z"}])

    def test_read_with_tag(self):
        history_data_driver = HistoryDataDriver(__class__.client)

        result = history_data_driver.read("points", 1495005255000000000, 1495005258000000000, {'country': 'Russia'})
        self.assertListEqual(result, [{'time': '2017-05-17T07:14:17Z', 'sum_traffic': 12345, 'country': 'Russia'}])

        result = history_data_driver.read("points", 1495005255000000000, 1495005258000000000, {'country': 'USA'})
        self.assertListEqual(result, [{'time': '2017-05-17T07:14:16Z', 'sum_traffic': 12345, 'country': 'USA'}])
