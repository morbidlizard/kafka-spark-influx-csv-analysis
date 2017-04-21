from .transformation_processor import TransformationProcessor

class Processor:
    def __init__(self, config):
        transformation_processor = TransformationProcessor(config.content["processing"]["transformation"])
        self.transformation = transformation_processor.transformation

    # should return lambda:
    def get_pipeline_processing(self):
        return self.transformation