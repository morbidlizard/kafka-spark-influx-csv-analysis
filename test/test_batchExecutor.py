from unittest import TestCase
from executors import BatchExecutor, StreamingExecutor
from pyspark.sql import SparkSession
from errors import  ExecutorError

import os

INPUT_PATH = os.path.join(os.path.dirname(__file__), os.path.join("data", "test.csv"))

class TestBatchExecutor(TestCase):
    def test___init__(self):
        spark = SparkSession.builder.getOrCreate()
        rdd = spark.read.csv(INPUT_PATH).rdd
        test_executor = BatchExecutor(rdd)

        self.assertIsInstance(test_executor, BatchExecutor,
                              "test_executor should be instance of BatchExecutor")

    def test_set_pipeline_processing(self):

        spark = SparkSession.builder.getOrCreate()
        rdd = spark.read.csv(INPUT_PATH).rdd
        test_executor = BatchExecutor(rdd)
        test_executor.set_pipeline_processing(lambda x: x.count())

        self.assertTrue(test_executor._action, "action should be set in set_pipeline_processing")

    def test_run_pipeline_processing(self):

        spark = SparkSession.builder.getOrCreate()
        rdd = spark.read.csv(INPUT_PATH).rdd
        test_executor = BatchExecutor(rdd)
        test_executor.set_pipeline_processing(lambda x: x.count())
        number_record = test_executor.run_pipeline()

        self.assertEqual(number_record, 5, "Result should be equal 5 (number of record in test file)")

        test_executor.set_pipeline_processing(None)

        with self.assertRaises(ExecutorError) as context:
            number_record = test_executor.run_pipeline()

        self.assertTrue("action and options" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

