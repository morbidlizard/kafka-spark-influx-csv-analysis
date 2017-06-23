# Copyright 2017, bwsoft management
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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