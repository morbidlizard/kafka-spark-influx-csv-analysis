from unittest import TestCase,skip
from errors.errors import KafkaConnectError
from input.executors import StreamingExecutor
from input.input_module import KafkaStreaming


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


@skip("The method of optimal testing is not determined. Requires server start with kafka")
class TestKafkaStreaming(TestCase):
    def test_getExutor(self):
        config = TestConfig({"server": "localhost", "port": 29092, "topic": "sflow", "batchDuration": 4, "sep": ","})
        test_read = KafkaStreaming(config.content)
        test_executor = test_read.get_streaming_executor()
        self.assertIsInstance(test_executor, StreamingExecutor,
                              "When ruse kafka streaming executor should be instance of StreamingExecutor")

        config = TestConfig({"server": "localhost", "port": 29091, "topic": "sflow", "batchDuration": 4, "sep": ","})
        with self.assertRaises(KafkaConnectError) as context:
            test_read = KafkaStreaming(config.content)
        self.assertTrue("Kafka error" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")
