from output_writer import OutputWriter

class CSVWriter(OutputWriter):
    def __init__(self, path, sep=";", encoding="utf-8"):
        OutputWriter.__init__(self, path)
        self.sep = sep
        self.encoding = encoding

    def write(self, rdd):
        data = rdd.toPandas()
        data.to_csv(self.path, self.sep, self.encoding)