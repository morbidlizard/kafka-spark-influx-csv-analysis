from mocks import ReaderFactoryMock, WriterFactoryMock, ProcessorMock

class Dispatcher:
    def __init__(self, config):
        self.executor = ReaderFactoryMock(config).getExecutor()
        self.writer = WriterFactoryMock().instance_writer(config)
        self.processor = ProcessorMock(config)

    def run_pipeline(self):
        processor_part = self.processor.get_pipeline_processing()
        self.executor.set_pipeline_processing(lambda rdd: self.writer.write(processor_part(rdd)))