import unittest
import os

from input_module import ReadCSVFile
from executors import BatchExecutor

class ReadCSVFileTestCase(unittest.TestCase):
    def test_getExutor(self):
        test_read = ReadCSVFile(os.path.join(os.path.dirname(__file__), os.path.join("data", "test.csv")))
        test_executor = test_read.get_batch_executor()

        self.assertIsInstance(test_executor, BatchExecutor,
                              "When read csv file executor should be instance of BatchExecutable")
