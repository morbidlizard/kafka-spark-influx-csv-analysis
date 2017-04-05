import unittest
import os

from csv_writer import CSVWriter
from pyspark.sql import SparkSession

class CSVWriterTestCase(unittest.TestCase):
    def tearDown(self):
        if os.path.exists("test/data/test_output.csv"):
            os.remove("test/data/test_output.csv")

    def test__init__(self):
        writer = CSVWriter("test/data/test_input.csv", ";", "utf-8")
        self.assertEqual(writer.encoding, "utf-8", "Encoding should be utf-8")
        self.assertEqual(writer.sep, ";", "Separator should be ';'")
        self.assertEqual(writer.path, "test/data/test_input.csv", "Path to file should be data/test_input.csv")


    def test_write(self):
        writer = CSVWriter("test/data/test_output.csv", ";", "utf-8")

        spark = SparkSession\
            .builder\
            .appName("LinesWriter")\
            .getOrCreate()

        dataframe = spark.read.csv("test/data/test_input.csv")

        writer.write(dataframe)
        spark.stop()

        self.assertTrue(os.path.exists("test/data/test_output.csv"), "Output file 'test_output.csv' should be created")
        self.assertGreater(os.stat("test/data/test_output.csv").st_size, 0, "File should be not empty" )