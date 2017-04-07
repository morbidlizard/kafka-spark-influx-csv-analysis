import unittest
from config import Config
from dispatcher import Dispatcher

from mocks import ExecutorMock, WriterMock, ProcessorMock

class DispatcherTestCase(unittest.TestCase):
    def test__init__(self):
        config = Config("test/data/config.json")
        dispatcher = Dispatcher(config)

        self.assertIsInstance(dispatcher.executor, ExecutorMock, "executor should has type ExecutorMock")
        self.assertTrue(hasattr(dispatcher.executor, "set_pipeline_processing"), "executor should has set_pipeline_processing method")

        self.assertIsInstance(dispatcher.processor, ProcessorMock, "processor should has type ProcessorMock")
        self.assertTrue(hasattr(dispatcher.processor, "get_pipeline_processing"),
                        "processor should has get_pipeline_processing method")

        self.assertIsInstance(dispatcher.writer, WriterMock, "Writer should has type WriterMock")
        self.assertTrue(hasattr(dispatcher.writer, "write"), "Writer should has write method")