from .transformation_creator import TransformationCreator
from config_parsing.transformations_validator import  TransformatoinsValidator
from config_parsing.transformations_parser import  TransformationsParser, FieldTransformation

class TransformationProcessor:
    def __init__(self, transformations):
        transformations_parser = TransformationsParser(transformations)
        transformations_parser.run()

        transformations_validator = TransformatoinsValidator()
        self.fields = transformations_validator.validate(transformations_parser.expanded_transformation)

        transformations_creator = TransformationCreator(transformations_parser.expanded_transformation)
        row_transformations = transformations_creator.build_lambda()
        self.transformation = lambda rdd: rdd.map(row_transformations)