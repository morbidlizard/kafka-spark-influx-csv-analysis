from config_parsing.transformations_parser import TransformationsParser
from config_parsing.transformations_validator import TransformatoinsValidator
from operations.transformation_operations import TransformationOperations
from .transformation_creator import TransformationCreator


class TransformationProcessor:
    def __init__(self, config):
        transformations_parser = TransformationsParser(config.content["processing"]["transformation"])
        transformations_parser.run()

        operations = TransformationOperations(config.content["databases"])

        transformations_validator = TransformatoinsValidator(operations, config.data_structure_pyspark)
        self.fields = transformations_validator.validate(transformations_parser.expanded_transformation)

        transformations_creator = TransformationCreator(config.data_structure,
                                                        transformations_parser.expanded_transformation, operations)
        row_transformations = transformations_creator.build_lambda()
        self.transformation = lambda rdd: rdd.map(row_transformations)
