import pyspark

from .output_writer import OutputWriter

class StdOutWriter(OutputWriter):
    def __init__(self): pass

    def get_write_lambda(self):
        return lambda rdd_or_object: \
            print( rdd_or_object.collect() if isinstance(rdd_or_object, pyspark.rdd.RDD) else rdd_or_object)