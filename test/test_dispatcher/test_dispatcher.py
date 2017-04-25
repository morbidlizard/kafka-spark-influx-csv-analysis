import os
import unittest

from config_parsing.config import Config
from dispatcher.dispatcher import Dispatcher
from input.executors import Executor
from processor.processor import  Processor
from output.output_writer import OutputWriter


class DispatcherTestCase(unittest.TestCase):
    def test__init__(self):
        config = Config(os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_dispatcher.json")))
        dispatcher = Dispatcher(config)

        self.assertIsInstance(dispatcher.executor, Executor, "executor should has type Executor")
        self.assertTrue(hasattr(dispatcher.executor, "set_pipeline_processing"), "executor should has set_pipeline_processing method")

        self.assertIsInstance(dispatcher.processor, Processor, "processor should has type Processor")
        self.assertTrue(hasattr(dispatcher.processor, "get_pipeline_processing"),
                        "processor should has get_pipeline_processing method")

        self.assertIsInstance(dispatcher.writer, OutputWriter, "Writer should has type WriterMock")
        self.assertTrue(hasattr(dispatcher.writer, "get_write_lambda"), "Writer should has get_write_lambda method")