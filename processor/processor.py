from .transformation_processor import TransformationProcessor
from .aggregation_processor import AggregationProcessor


class Processor:
    def __init__(self, config):
        transformation_processor = TransformationProcessor(config.content["processing"]["transformation"])
        self.transformation = transformation_processor.transformation

        self.transformation_processor_fields = transformation_processor.fields
        aggregation_processor = AggregationProcessor(config, transformation_processor.fields)

        # for example: max_packet_size, sum_traffic
        self.aggregation_output_struct = ["{0}_{1}".format(aggr.lower(), field) for field, aggr in aggregation_processor._field_to_func_name.items()]
        self.aggregation = aggregation_processor.get_aggregation_lambda()

    # should return lambda:
    def get_pipeline_processing(self):
        return lambda rdd: self.aggregation(self.transformation(rdd))
