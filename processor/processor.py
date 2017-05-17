from .transformation_processor import TransformationProcessor
from .aggregation_processor import AggregationProcessor


class Processor:
    def __init__(self, config):
        transformation_processor = TransformationProcessor(config.content["processing"]["transformation"])
        self.transformation = transformation_processor.transformation

        self.transformation_processor_fields = transformation_processor.fields
        aggregation_processor = AggregationProcessor(config, transformation_processor.fields)

        self.aggregation_output_struct = aggregation_processor.get_output_structure()

        self.aggregation = aggregation_processor.get_aggregation_lambda()
        self.enumerate_output_aggregation_field = aggregation_processor.get_enumerate_field()

    # should return lambda:
    def get_pipeline_processing(self):
        return lambda rdd: self.aggregation(self.transformation(rdd))
