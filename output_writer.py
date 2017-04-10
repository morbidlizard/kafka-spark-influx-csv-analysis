class OutputWriter:
    def __init__(self, path):
        self.path = path

    def write(self, rdd):
        print("Override me in child classes!")