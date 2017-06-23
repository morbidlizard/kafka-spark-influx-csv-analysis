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

import pyspark

from .output_writer import OutputWriter


class StdOutWriter(OutputWriter):
    def __init__(self):
        pass

    def get_write_lambda(self):
        def print_result(rdd_or_object):
            print('---------------------------')
            if isinstance(rdd_or_object, pyspark.rdd.RDD):
                for field in rdd_or_object.collect():
                    print(field)
            else:
                print(rdd_or_object)

        return print_result
