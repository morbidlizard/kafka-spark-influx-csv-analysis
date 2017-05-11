import os
import unittest

import errors

from output.csv_writer import CSVWriter
from output.output_config import OutputConfig
from output.writer_factory import WriterFactory

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config.json"))
INCORRECT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "bad_config.json"))

class WriterFactoryTestCase(unittest.TestCase):
    def test_instance_writer(self):
        factory = WriterFactory()
        config = OutputConfig(CONFIG_PATH)

        writer = factory.instance_writer(config,list())
        self.assertIsInstance(writer, CSVWriter,"Writer should be instance of CSVWriter")

    def test_unsupported_output_format_exception_instance_writer(self):
        factory = WriterFactory()
        config = OutputConfig(INCORRECT_CONFIG_PATH)

        with self.assertRaises(errors.UnsupportedOutputFormat):
            factory.instance_writer(config,list())
