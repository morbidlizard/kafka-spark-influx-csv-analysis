import nanotime
from pyspark import rdd

from collections import Iterable
from .output_writer import OutputWriter


class InfluxWriter(OutputWriter):
    def __init__(self, client, database, measurement, input_fields, enumerate_input_field):
        # name of output field. for example: max_packet_size, sum_traffic
        self.input_rule = input_fields["rule"]
        fields = enumerate_input_field
        self.client, self.measurement, self.fields = client, measurement, fields
        self.client.create_database(database)

    def get_write_lambda(self):
        client, fields_mapping, measurement = self.client, self.fields, self.measurement
        key_field = list(map(lambda x: x["input_field"], filter(lambda x: x["key"], self.input_rule))) # need comment

        def make_points_from_partition(iterator):
            points = []
            for t in iterator: # need comment about struct t
                tags = dict(map(lambda x, y: (x, y), key_field, t[0])) # need comments
                value = t[1:]
                fields = dict(map(lambda x: (x, value[fields_mapping[x]]), fields_mapping.keys())) # need comments
                points.append({"measurement": measurement, "fields": fields,
                               "time": nanotime.now().nanoseconds(), "tags": tags})
            return points

        def make_points_from_tuple_or_number(object):
            t = object if isinstance(object, Iterable) else [object]  # tuple or number
            value = t
            fields = dict(map(lambda x: (x, value[fields_mapping[x]]), fields_mapping.keys())) # need comment about struct
            # fields = {fields_mapping[index]: value for index, value in enumerate(t)}
            return [{"measurement": measurement, "fields": fields, "time": nanotime.now().nanoseconds()}]

        def run_necessary_lambda(rdd_or_object):
            if isinstance(rdd_or_object, rdd.RDD):
                # make separate method?
                return (lambda rdd: rdd.foreachPartition(
                    lambda iterator: client.write_points(make_points_from_partition(iterator))))(rdd_or_object)
            else:
                # make separate method?
                return (lambda object: client.write_points(make_points_from_tuple_or_number(object)))(rdd_or_object)

        return lambda rdd_or_object: run_necessary_lambda(rdd_or_object)
