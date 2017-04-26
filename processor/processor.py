from .transformation_processor import TransformationProcessor
from .aggregation_processor import AggregationProcessor

class Processor:
    def __init__(self, config):
        transformation_processor = TransformationProcessor(config.content["processing"]["transformation"])
        self.transformation = transformation_processor.transformation

        aggregation_processor = AggregationProcessor(config, transformation_processor.fields)

        a = aggregation_processor.get_aggregation_lambda()
        self.aggregation = lambda rdd: rdd.reduce(a)


    # should return lambda:
    def get_pipeline_processing(self):
        return lambda rdd: self.aggregation(self.transformation(rdd))