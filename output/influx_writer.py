from .output_writer import OutputWriter

class InfluxWriter(OutputWriter):
    def write(self, rdd):
        print("I should write data to influx")