import json
import os
from pyspark.sql import types


class Config:
    def __init__(self, path_to_config):
        self.path = path_to_config
        with open(path_to_config) as cfg:
            self.content = json.load(cfg)
        with open(self.content["input"]["data_structure"]) as cfg:
            data_structure = json.load(cfg)

        self.data_structure = data_structure
        data_structure_list = list(map(lambda x: (x, data_structure[x]), data_structure.keys()))
        data_structure_sorted = sorted(data_structure_list, key=lambda x: x[1]["index"])
        self.data_structure_pyspark = types.StructType(
            list(map(lambda x: types.StructField(x[0], getattr(types, x[1]["type"])()),
                     data_structure_sorted)))
