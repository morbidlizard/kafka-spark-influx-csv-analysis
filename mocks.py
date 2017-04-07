class ExecutorMock:
    def set_pipeline_processing(self, handler):
        #do smth
        data = "mock data"
        handler(data)

class ReaderFactoryMock:
    def __init__(self, config):
        self.config = config

    def getExecutor(self):
      return ExecutorMock()

class WriterMock:
    def write(self, data):
        print("Write some data: ", data)

class WriterFactoryMock:
    def instance_writer(self, config):
        return WriterMock()

class ProcessorMock:
    def __init__(self, config):
        self.config = config

    def get_pipeline_processing(self):
        return lambda x: x + "!"