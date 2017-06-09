import errors
import json
import re

class FieldTransformation:
    def __init__(self, field_name, operation):
        self.field_name = field_name  # new field name
        self.operation = operation  # SyntaxTree or string

class SyntaxTree:
    def __init__(self):
        self.operation = None
        self.children = [] # list syntax trees or strings

    def append_child(self, child):
        self.children += [child]

    def show(self, shift): # helper method, later should be deleted?
        print(" " * shift * 2 + "operation: {}, has {} children: ".format(self.operation, len(self.children)))
        for ch in self.children:
            if isinstance(ch, SyntaxTree):
                ch.show(shift + 1)
            else:
                print(" " * (shift + 1) * 2 + "Leaf node: ", ch)

class TransformationsParser:
    def __init__(self, transformations):
        self.transformations = transformations
        self.expanded_transformation = [] # string or FieldTransformation

    def _parse(self, args): # parse function or field expression
        result = re.search(r'(\w+)\((.*)\)', args) # try match function
        tree = SyntaxTree()

        if result is not None:
            tree.operation, arguments = result.groups()
            index, start_index, end_index = 0, 0, 0

            while index < len(arguments):
                if arguments[index] == "(":  # find list function arguments
                    open_bracket, close_bracket = 1, 0
                    for i in range(index + 1, len(arguments)):
                        if arguments[i] == "(":
                            open_bracket += 1
                        elif arguments[i] == ")":
                            close_bracket += 1

                        if open_bracket == close_bracket:
                            end_index = i # index last ")"

                            child = self._parse(arguments[start_index:end_index + 1]) # extract arguments list
                            tree.append_child(child)

                            start_index = index = i # (equal end_index)
                            if i + 2 < len(arguments) and arguments[i + 1] == ",": # if we have smth after this function
                                start_index = i + 2  # eat 2 and start from new
                                index = i + 1

                            break

                    if open_bracket != close_bracket:
                        raise errors.IncorrectExpression(
                            "Incorrect expression: {} open brackets and {} close brackets ".format(open_bracket,
                                                                                                   close_bracket))

                elif arguments[index] == ",": # extract field name from list like param1,param2,param3
                    end_index = index
                    child = self._parse(arguments[start_index:end_index])
                    tree.append_child(child)
                    start_index = end_index + 1

                index += 1

            if end_index < len(arguments) - 1:
                child = self._parse(arguments[start_index:len(arguments)])
                tree.append_child(child)

            return tree
        else:
            return args # return field name

    def run(self):
        tokenized_transformation = list(map(lambda field: field.strip(), self.transformations.split(";")))
        for token in tokenized_transformation:
            if ":" not in token:  # if it's field
                self.expanded_transformation.append(token.strip())
            else:  # sum/minus/div and etc operations
                new_field, field_or_expression = list(map(lambda t: t.strip(), token.split(":")))
                self.expanded_transformation.append(FieldTransformation(new_field, self._parse(field_or_expression)))

# this class necessary only for test
class TransformationsParserConfig:
    def __init__(self, path_to_config):
        self.path = path_to_config
        with open(path_to_config) as cfg:
            self.content = json.load(cfg)
