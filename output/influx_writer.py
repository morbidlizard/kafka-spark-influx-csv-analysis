from .output_writer import OutputWriter

class InfluxWriter(OutputWriter):
    def get_write_lambda(self):
        print("I should write data to influx")