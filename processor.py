class Processor:
    def __init__(self, config):
        self.transform_rules = sorted(config.content["processing"], key=lambda item: item["id"])
        self.transform_rules = [ (lambda row: row) for item in self.transform_rules]

        def chain_builder(rdd):
            for transformation in self.transform_rules:
                rdd = rdd.map(transformation)

            return rdd

        self.chain = chain_builder

    # should return lambda:
    def get_pipeline_processing(self):
        return lambda rdd: self.chain(rdd)