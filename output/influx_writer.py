import nanotime
from pyspark import rdd

from collections import Iterable
from .output_writer import OutputWriter


class InfluxWriter(OutputWriter):
    def __init__(self, client, database, measurement, fields):
        self.client, self.measurement, self.fields = client, measurement, fields
        self.client.create_database(database)

    def get_write_lambda(self):
        client, fields_mapping, measurement = self.client, self.fields, self.measurement
        def make_points_from_partition(iterator):
            points = []
            for t in iterator:
                fields = {fields_mapping[index]: value for index, value in enumerate(t[1:])}
                points.append({"measurement": measurement, "fields": fields,
                               "time": nanotime.now().nanoseconds(), "tags": {"key": t[0]}})
            return points

        def make_points_from_tuple_or_number(object):
            t = object if isinstance(object, Iterable) else [object]  # tuple or number
            fields = {fields_mapping[index]: value for index, value in enumerate(t)}
            return [{"measurement": measurement, "fields": fields, "time": nanotime.now().nanoseconds()}]

        def run_necessary_lambda(rdd_or_object):
            if isinstance(rdd_or_object, rdd.RDD):
                return (lambda rdd: rdd.foreachPartition(
                            lambda iterator: client.write_points(make_points_from_partition(iterator))))(rdd_or_object)
            else:
                return (lambda object: client.write_points(make_points_from_tuple_or_number(object)))(rdd_or_object)

        return lambda rdd_or_object: run_necessary_lambda(rdd_or_object)
