import unittest
from unittest import mock
from unittest.mock import MagicMock

from input.executors import BatchExecutor
from input.input_module import ReadCSVFile


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class ReadCSVFileTestCase(unittest.TestCase):
    @mock.patch('pyspark.sql.session.SparkSession', autospec=True)
    def test_getExutor(self, mock_sparksession):
        mock_context = MagicMock()
        mock_context.addFile.return_value = "test"
        mock_spark = MagicMock()
        mock_spark.sparkContext.return_value = mock_context
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark
        mock_sparksession.builder
        mock_sparksession.builder.return_value = mock_builder
        config = TestConfig({"input": {
            "input_type": "csv",
            "options": {
                "filename": "test/data/test.csv"
            }
        },
            "databases": {
                "country": "./GeoLite2/GeoLite2-Country.mmdb",
                "city": "./GeoLite2/GeoLite2-City.mmdb",
                "asn": "./GeoLite2/GeoLite2-ASN.mmdb"
            }})
        test_read = ReadCSVFile(config.content)
        test_executor = test_read.get_batch_executor()

        self.assertIsInstance(test_executor, BatchExecutor,
                              "When read csv file executor should be instance of BatchExecutable")
