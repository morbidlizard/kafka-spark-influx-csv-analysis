from unittest import TestCase, mock

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming import DStream
from errors import ExecutorError
from input.executors import StreamingExecutor


class TestStreamingExecutor(TestCase):
    @mock.patch('pyspark.streaming.DStream')
    @mock.patch('pyspark.streaming.StreamingContext')
    def test_run_pipeline(self, mock_streaming_context, mock_dstream):
        test_executor = StreamingExecutor(mock_dstream, mock_streaming_context)
        test_function = lambda rdd: rdd
        test_executor.set_pipeline_processing(test_function)
        test_executor.run_pipeline()

        mock_dstream.foreachRDD.assert_called_with(test_function)

        self.assertTrue(mock_streaming_context.start.called, "Failed streaming. The method 'start' didn't call.")
        self.assertTrue(mock_streaming_context.awaitTermination.called,
                        "Failed streaming. The method 'awaitTermination' didn't call.")
        test_executor.set_pipeline_processing(None)
        with self.assertRaises(ExecutorError) as context:
            test_executor.run_pipeline()

        self.assertTrue("action and options" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

    @mock.patch('pyspark.streaming.DStream')
    @mock.patch('pyspark.streaming.StreamingContext')
    def test_set_pipeline_processing(self, mock_streaming_context, mock_dstream):
        test_executor = StreamingExecutor(mock_dstream, mock_streaming_context)
        test_function = lambda x: x.count()
        test_executor.set_pipeline_processing(test_function)

        self.assertTrue(test_executor._action, "action should be set in set_pipeline_processing")

        self.assertEqual(test_function, test_executor._action,
                         "field _action after set_pipeline_processing should be equal inpud action")
