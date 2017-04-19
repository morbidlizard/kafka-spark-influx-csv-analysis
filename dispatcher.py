from inpute_module import ReadFactory
from writer_factory import WriterFactory
from transformations_parser import TransformationsParser
from transformations_validator import TransformatoinsValidator
from aggregations_parser import AggregationsParser
from mocks import ProcessorMock


class Dispatcher:
    def __init__(self, config):
        self.executor = ReadFactory(config).get_executor()
        self.writer = WriterFactory().instance_writer(config)
        self.transformations_parser = TransformationsParser(config)
        self.transformations_parser.run()
        self.transformations_validator = TransformatoinsValidator()
        self.result_validation = self.transformations_validator.validate(self.transformations_parser.expanded_transformation)
        self.agregation_parser = AggregationsParser(config, self.result_validation)
        self.processor = ProcessorMock(config)


    def run_pipeline(self):
        processor_part = self.processor.get_pipeline_processing()
        self.executor.set_pipeline_processing(lambda rdd: self.writer.write(processor_part(rdd)))