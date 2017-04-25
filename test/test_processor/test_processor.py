import os
import types
import unittest

from processor.processor import Processor
from config_parsing.config import Config

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "config_processor.json"))

class ProcessorTestCase(unittest.TestCase):
    def test__init__(self):
        config = Config(CONFIG_PATH)
        p = Processor(config)
        self.assertIsInstance(p.transformation, types.LambdaType, "Processor#transformation should be a lambda object")