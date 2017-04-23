from unittest import TestCase

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

from errors import ExecutorError
from input.executors import StreamingExecutor


def help_accumulator(rdd, sum_elements, num_zero_rdd):
    rdd.foreach(lambda x: sum_elements.add(x))
    num_zero_rdd.add(rdd.count())


class TestStreamingExecutor(TestCase):
    def setUp(self):
        conf = SparkConf().setAppName("UnitTest")
        # self._sc = SparkContext(conf=conf)
        self._spark = SparkSession.builder \
            .appName("TestStreamingExecutor") \
            .getOrCreate()

        self._ssc = StreamingContext(self._spark.sparkContext, 1)
        rdd_queue = []
        for i in range(3):
            rdd_queue += [self._ssc.sparkContext.parallelize([j for j in range(1, 5)], 5)]
        test_dstream = self._ssc.queueStream(rdd_queue)
        self._test_stream = self._ssc.queueStream(rdd_queue)

    def tearDown(self):
        self._spark.stop()

    def test___init__(self):
        test_executor = StreamingExecutor(self._test_stream)
        self.assertIsInstance(test_executor, StreamingExecutor,
                              "test_executor should be instance of BatchExecutor")

    def test_run_pipeline(self):
        test_executor = StreamingExecutor(self._test_stream)
        test_executor.set_pipeline_processing(lambda rdd: help_accumulator(rdd, sum_elements, num_zero_rdd))
        test_executor.run_pipeline()
        sum_elements = self._spark.sparkContext.accumulator(0)
        num_zero_rdd = self._spark.sparkContext.accumulator(0)

        self._ssc.start()
        while num_zero_rdd.value == 0:
            pass
        self._ssc.stop(stopSparkContext=True, stopGraceFully=True)

        self.assertEqual(sum_elements.value, 30, "Result should be equal 30 (The elements sum of all RDD in dstream)")

        test_executor.set_pipeline_processing(None)
        with self.assertRaises(ExecutorError) as context:
            number_record = test_executor.run_pipeline()

        self.assertTrue("action and options" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    def test_set_pipeline_processing(self):
        test_executor = StreamingExecutor(self._test_stream)
        test_executor.set_pipeline_processing(lambda x: x.count())

        self.assertTrue(test_executor._action, "action should be set in set_pipeline_processing")
