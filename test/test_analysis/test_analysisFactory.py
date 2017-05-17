from unittest import TestCase
from pyspark.sql.types import StructType, StructField, LongType
from analysis.analysis_factory import AnalysisFactory
from analysis.ianalysis import IAnalysis


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestAnalysisFactory(TestCase):
    def test_instance_analysis(self):
        input_data_structure = StructType([StructField("packet_size", LongType()),
                                           StructField("traffic", LongType()),
                                           StructField("ip_size", LongType()),
                                           StructField("ip_size_sum", LongType()),
                                           ])
        config = TestConfig(
            {
                "processing": {
                    "transformation": "packet_size;traffic: mult(packet_size,sampling_rate);ip_size;ip_size_sum:ip_size",
                    "aggregations": {
                        "operation_type": "reduce",
                        "rule": " Max(packet_size), Sum(traffic), Min(ip_size), Sum(ip_size_sum)"
                    }
                },
                "analysis": {
                    "historical": {
                        "method": "mock",
                        "options": {
                            "deviation": 20
                        }
                    },
                    "alert": {
                        "method": "stdout",
                        "option": {}
                    },
                    "time_delta": 20,
                    "accuracy": 3,
                    "rule": {
                        "ip_size": 5,
                        "ip_size_sum": 10,
                        "traffic": 15
                    }
                }
            })
        factory = AnalysisFactory(config, input_data_structure)
        analyses = factory.instance_analysis()
        self.assertIsInstance(analyses, IAnalysis, "Writer should be instance of IAnalysis")
