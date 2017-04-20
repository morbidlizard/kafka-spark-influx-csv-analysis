class Processor:
    def __init__(self, config):
        self.transform_rules = [(lambda row: row) for item in range(0,3)]

        def chain_builder(rdd):
            for transformation in self.transform_rules:
                rdd = rdd.map(transformation)

            return rdd

        self.chain = chain_builder

    # should return lambda:
    def get_pipeline_processing(self):
        return lambda rdd: self.chain(rdd)