import os
import unittest

from input.executors import BatchExecutor
from input.input_module import ReadCSVFile


class ReadCSVFileTestCase(unittest.TestCase):
    def test_getExutor(self):
        test_read = ReadCSVFile(os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "test.csv")))
        test_executor = test_read.get_batch_executor()

        self.assertIsInstance(test_executor, BatchExecutor,
                              "When read csv file executor should be instance of BatchExecutable")
