import pyspark.sql.types as types
from processor.geo_operations import country, city, aarea

class TransformationOperations:
    def __init__(self, geoip_paths):
        self.operations_dict = {
            "sum": {
                "operands": 2,
                "types": [types.LongType(), types.LongType()],
                "result": types.LongType(),
                "lambda": lambda x, y: x + y
            },
            "minus": {
                "operands": 2,
                "types": [types.LongType(), types.LongType()],
                "result": types.LongType(),
                "lambda": lambda x, y: x - y
            },
            "mult": {
                "operands": 2,
                "types": [types.LongType(), types.LongType()],
                "result": types.LongType(),
                "lambda": lambda x, y: x * y
            },
            "div": {
                "operands": 2,
                "types": [types.LongType(), types.LongType()],
                "result": types.LongType(),
                "lambda": lambda x, y: x / y
            },
            "country": {
                "operands": 1,
                "types": [types.StringType()],
                "result": types.StringType(),
                "lambda": lambda ip: country(ip, geoip_paths["country"])
            },
            "city": {
                "operands": 1,
                "types": [types.StringType()],
                "result": types.StringType(),
                "lambda": lambda ip: city(ip, geoip_paths["city"])
            },
            "aarea": {
                "operands": 1,
                "types": [types.StringType()],
                "result": types.StringType(),
                "lambda": lambda ip: aarea(ip, geoip_paths["asn"])
            },
            "truncate": {
                "operands": 2,
                "types": [types.StringType(), types.LongType()],
                "result": types.StringType(),
                "lambda": lambda long_string,length: long_string[:length]
            }
        }