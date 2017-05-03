from input.input_module import ReadFactory
from processor.processor import Processor
from output.writer_factory import WriterFactory

class Dispatcher:
    def __init__(self, config):
        self.executor = ReadFactory(config).get_executor()
        self.writer = WriterFactory().instance_writer(config)

        self.processor = Processor(config)


    def run_pipeline(self):
        processor_part = self.processor.get_pipeline_processing()

        write_lambda = self.writer.get_write_lambda()
        pipeline = lambda rdd: write_lambda(processor_part(rdd))

        self.executor.set_pipeline_processing(pipeline)
        self.executor.run_pipeline()

    def stop_pipeline(self):
        self.executor.stop_pipeline()
