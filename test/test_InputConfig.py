import unittest
import os
from inpute_module import InputConfig


class InputConfigTestCase(unittest.TestCase):
    def test__init__(self):
        config = InputConfig(os.path.join(os.path.dirname(__file__), os.path.join("data", "input_config_csv.json")))
        self.assertIsInstance(config, InputConfig, "Config should be instance InputConfig")
