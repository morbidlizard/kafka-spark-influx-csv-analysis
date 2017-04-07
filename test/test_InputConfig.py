import unittest

from inpute_module import InputConfig


class InputConfigTestCase(unittest.TestCase):
    def test__init__(self):
        config = InputConfig("data/input_config_csv.json")
        self.assertIsInstance(config, InputConfig, "Config should be instance InputConfig")
