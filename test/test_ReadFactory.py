import unittest

from inpute_module import ReadFactory, InputConfig
from executors import BatchExecutor
from error import InputError


class ReadFactoryTestCase(unittest.TestCase):
    def test_getExutor(self):
        config = InputConfig("data/input_config_csv.json")
        factory = ReadFactory(config)
        test_executor = factory.get_executor()

        self.assertIsInstance(test_executor, BatchExecutor,
                              "When read csv file executor should be instance of BatchExecutable")

    def test_exeption_on_error1_in_input_config(self):
        config = InputConfig("data/bad1_input_config.json")
        factory = ReadFactory(config)

        with self.assertRaises(InputError) as context:
            factory.get_executor()

        self.assertTrue("Some option was miss" in context.exception.args[0],
                        "Catch exeception, but it differs from test exception")

    def test_exeption_on_error2_in_input_config(self):
        config = InputConfig("data/bad2_input_config.json")
        factory = ReadFactory(config)

        with self.assertRaises(InputError) as context:
            factory.get_executor()

        self.assertTrue("unsuported input format" in context.exception.args[0],
                        "Catch exeception, but it differs from test exception")
