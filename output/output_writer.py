class OutputWriter:
    def __init__(self, path):
        self.path = path

    def get_write_lambda(self):
        raise NotImplementedError("Write method should be overrided!")