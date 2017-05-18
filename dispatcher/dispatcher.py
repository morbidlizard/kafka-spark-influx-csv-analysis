from analysis.analysis_factory import AnalysisFactory
from input.input_module import ReadFactory
from output.writer_factory import WriterFactory
from processor.processor import Processor


class Dispatcher:
    def __init__(self, config):
        self.executor = ReadFactory(config).get_executor()
        self.processor = Processor(config)
        self.writer = WriterFactory().instance_writer(config, self.processor.aggregation_output_struct)
        self._isAnalysis = False
        if ("analysis" in config.content.keys()):
            self._isAnalysis = True
            self.analysis = AnalysisFactory(config,
                                            self.processor.enumerate_output_aggregation_field).instance_analysis()

    def run_pipeline(self):
        processor_part = self.processor.get_pipeline_processing()

        write_lambda = self.writer.get_write_lambda()
        # pipeline = lambda rdd: write_lambda(processor_part(rdd))
        if (self._isAnalysis):
            analysis_lambda = self.analysis.get_analysis_lambda()
            pipeline = lambda rdd: self._all_pipeline(rdd, processor_part, write_lambda, analysis_lambda)
        else:
            analysis_lambda = lambda x: x
            pipeline = lambda rdd: self._all_pipeline(rdd, processor_part, write_lambda, analysis_lambda)

        self.executor.set_pipeline_processing(pipeline)
        self.executor.run_pipeline()

    def _all_pipeline(self, rdd, processor_part, write_part, analysis_part):
        processed = processor_part(rdd)
        write_part(processed)
        analysis_part(processed)

    def stop_pipeline(self):
        self.executor.stop_pipeline()
