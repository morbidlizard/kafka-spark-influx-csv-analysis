import pyspark
import pandas

from collections import Iterable
from pyspark.sql import SparkSession
from .output_writer import OutputWriter


class CSVWriter(OutputWriter):
    def __init__(self, path, sep=";", encoding="utf-8"):
        OutputWriter.__init__(self, path)
        self.sep = sep
        self.encoding = encoding
        self.spark = SparkSession.builder.getOrCreate()

    def get_write_lambda(self):
        def make_data(rdd_or_object):
            if isinstance(rdd_or_object, pyspark.rdd.RDD):
                data = self.spark.createDataFrame(rdd_or_object).toPandas()
            else:
                data = pandas.DataFrame.from_records(
                    [rdd_or_object] if isinstance(rdd_or_object, Iterable) else [(rdd_or_object,)])

            return data

        path, sep, encoding = self.path, self.sep, self.encoding

        return lambda rdd_or_object: make_data(rdd_or_object).to_csv(path, sep, encoding)
