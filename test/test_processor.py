import unittest
import types
import os

from processor import Processor
from processor_config import ProcessorConfig

CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.path.join("data", "config.json"))

class ProcessorTestCase(unittest.TestCase):
    def test__init__(self):
        p_config = ProcessorConfig(CONFIG_PATH)
        p = Processor(p_config)
        self.assertIsInstance(p.transform_rules, list, "Processor#transform_rules should be a list object")
        for item in p.transform_rules:
            self.assertIsInstance(item, types.FunctionType, "Each element should be function")

        self.assertIsInstance(p.chain, types.FunctionType, "Processor#chain should be a function")

    def test_get_pipeline_processing(self):
        p_config = ProcessorConfig(CONFIG_PATH)
        p = Processor(p_config)

        chain = p.get_pipeline_processing()
        self.assertIsInstance(chain, types.LambdaType, "Processor#get_pipeline_processing should return a function")