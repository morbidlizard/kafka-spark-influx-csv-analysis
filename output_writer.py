class OutputWriter:
    def __init__(self, path):
        self.path = path

    def write(self, rdd):
        raise NotImplementedError("Write method should be overrided!")