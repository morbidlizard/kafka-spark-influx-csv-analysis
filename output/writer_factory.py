from errors import errors
from influxdb import InfluxDBClient
from .std_out_writer import StdOutWriter
from .influx_writer import InfluxWriter


class WriterFactory:
    def instance_writer(self, output_config, struct, enumerate_input_field):
        output = output_config.content["output"]
        if output["method"] == "influx":
            client = InfluxDBClient(output["options"]["influx"]["host"], output["options"]["influx"]["port"],
                                    output["options"]["influx"]["username"], output["options"]["influx"]["password"],
                                    output["options"]["influx"]["database"])

            return InfluxWriter(client, output["options"]["influx"]["database"],
                                output["options"]["influx"]["measurement"], struct, enumerate_input_field)
        elif output["method"] == "stdout":
            return StdOutWriter()

        raise errors.UnsupportedOutputFormat("Format {} not supported".format(output["method"]))
