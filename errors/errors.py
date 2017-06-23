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


class KafkaConnectError(BaseException):
    pass


class UnsupportedAnalysisFormat(BaseException):
    pass
