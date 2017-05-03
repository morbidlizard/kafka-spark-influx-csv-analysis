import pyspark

from .output_writer import OutputWriter


class StdOutWriter(OutputWriter):
    def __init__(self):
        pass

    def get_write_lambda(self):
        def print_result(rdd_or_object):
            print('---------------------------')
            if isinstance(rdd_or_object, pyspark.rdd.RDD):
                for field in rdd_or_object.collect():
                    print(field)
            else:
                print(rdd_or_object)

        return print_result
