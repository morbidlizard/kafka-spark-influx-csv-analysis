import errors
from influxdb import InfluxDBClient

from .csv_writer import CSVWriter
from .std_out_writer import StdOutWriter
from .influx_writer import InfluxWriter


class WriterFactory:
    def instance_writer(self, output_config, struct, enumerate_input_field):
        output = output_config.content["output"]
        if output["method"] == "csv":
            return CSVWriter(output["options"]["csv"]["path"], output["options"]["csv"]["sep"],
                             output["options"]["csv"]["encoding"])
        elif output["method"] == "influx":
            client = InfluxDBClient(output["options"]["influx"]["host"], output["options"]["influx"]["port"],
                                    output["options"]["influx"]["username"], output["options"]["influx"]["password"],
                                    output["options"]["influx"]["database"])

            return InfluxWriter(client, output["options"]["influx"]["database"],
                                output["options"]["influx"]["measurement"], struct, enumerate_input_field)
        elif output["method"] == "stdout":
            return StdOutWriter()

        raise errors.UnsupportedOutputFormat("Format {} not supported".format(output["method"]))
