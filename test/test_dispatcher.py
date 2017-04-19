import os
import unittest

from config import Config
from dispatcher import Dispatcher
from executors import Executor
from output_writer import OutputWriter
from mocks import ExecutorMock, WriterMock, ProcessorMock

class DispatcherTestCase(unittest.TestCase):
    def test__init__(self):
        config = Config(os.path.join(os.path.dirname(__file__), os.path.join("data", "config.json")))
        dispatcher = Dispatcher(config)

        self.assertIsInstance(dispatcher.executor, Executor, "executor should has type Executor")
        self.assertTrue(hasattr(dispatcher.executor, "set_pipeline_processing"), "executor should has set_pipeline_processing method")

        self.assertIsInstance(dispatcher.processor, ProcessorMock, "processor should has type ProcessorMock")
        self.assertTrue(hasattr(dispatcher.processor, "get_pipeline_processing"),
                        "processor should has get_pipeline_processing method")

        self.assertIsInstance(dispatcher.writer, OutputWriter, "Writer should has type WriterMock")
        self.assertTrue(hasattr(dispatcher.writer, "write"), "Writer should has write method")