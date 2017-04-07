import unittest

from inpute_module import ReadCSVFile
from executors import BatchExecutor


class ReadCSVFileTestCase(unittest.TestCase):
    def test_getExutor(self):
        test_read = ReadCSVFile("data/test.csv")
        test_executor = test_read.get_batch_executor()

        self.assertIsInstance(test_executor, BatchExecutor,
                              "When read csv file executor should be instance of BatchExecutable")
