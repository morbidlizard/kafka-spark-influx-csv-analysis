import re
import errors
from csv_writer import CSVWriter

class WriterFactory:
    def instance_writer(self, output_config):
        # TODO make more pretty?
        if re.search(r'^.*?\.csv$', output_config.content["output"]) is not None:
            return CSVWriter(output_config.content["output"], output_config.content["csv"]["sep"], output_config.content["csv"]["encoding"])

        raise errors.UnsupportedOutputFormat("Writing to file {} not supported".format(output_config.content["output"]))
