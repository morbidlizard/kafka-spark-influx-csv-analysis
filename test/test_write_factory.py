import unittest
import errors

from output_config import OutputConfig
from csv_writer import CSVWriter
from writer_factory import WriterFactory

class WriterFactoryTestCase(unittest.TestCase):
    def test_instance_writer(self):
        factory = WriterFactory()
        config = OutputConfig("test/data/config.json")

        writer = factory.instance_writer(config)
        self.assertIsInstance(writer, CSVWriter,"Writer should be instance of CSVWriter")

    def test_unsupported_output_format_exception_instance_writer(self):
        factory = WriterFactory()
        config = OutputConfig("test/data/bad_config.json")

        with self.assertRaises(errors.UnsupportedOutputFormat):
            factory.instance_writer(config)
