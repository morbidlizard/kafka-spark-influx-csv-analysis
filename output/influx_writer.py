import nanotime
from pyspark import rdd

from collections import Iterable
from .output_writer import OutputWriter


class InfluxWriter(OutputWriter):
    def __init__(self, client, database, measurement, input_fields):
        # name of output field. for example: max_packet_size, sum_traffic
        self.input_rule = input_fields["rule"]
        fields = list(map(lambda x: x["input_field"], filter(lambda x: not x["key"], self.input_rule)))

        self.client, self.measurement, self.fields = client, measurement, fields
        self.client.create_database(database)

    def get_write_lambda(self):
        client, fields_mapping, measurement = self.client, self.fields, self.measurement
        key_field = list(map(lambda x: x["input_field"], filter(lambda x: x["key"], self.input_rule)))

        def make_points_from_partition(iterator):
            points = []
            for t in iterator:
                tags = dict(map(lambda x, y: (x, y), key_field, t[0]))
                fields = {fields_mapping[index]: value for index, value in enumerate(t[1:])}
                points.append({"measurement": measurement, "fields": fields,
                               "time": nanotime.now().nanoseconds(), "tags": tags})
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
