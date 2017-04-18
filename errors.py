class UnsupportedOutputFormat(BaseException):
    pass

class IncorrectExpression(BaseException):
    pass

class FieldNotExists(BaseException):
    pass

class OperationNotSupportedError(BaseException):
    pass

class IncorrectArgumentsAmountForOperationError(BaseException):
    pass

class IncorrectArgumentTypeForOperationError(BaseException):
    pass

class NotValidAggregationExpression(BaseException):
    pass

class InputError(BaseException):
    pass


class ExecutorError(BaseException):
    pass
