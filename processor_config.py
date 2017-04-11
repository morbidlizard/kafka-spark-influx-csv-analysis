import json

class ProcessorConfig:
    def __init__(self, path_to_config):
        self.path = path_to_config
        with open(path_to_config) as cfg:
            self.content = json.load(cfg)