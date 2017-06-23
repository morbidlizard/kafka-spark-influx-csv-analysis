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

from .transformation_processor import TransformationProcessor
from .aggregation_processor import AggregationProcessor


class Processor:
    def __init__(self, config):
        transformation_processor = TransformationProcessor(config)
        self.transformation = transformation_processor.transformation

        self.transformation_processor_fields = transformation_processor.fields
        aggregation_processor = AggregationProcessor(config, transformation_processor.fields)

        self.aggregation_output_struct = aggregation_processor.get_output_structure()

        self.aggregation = aggregation_processor.get_aggregation_lambda()
        self.enumerate_output_aggregation_field = aggregation_processor.get_enumerate_field()

    # should return lambda:
    def get_pipeline_processing(self):
        return lambda rdd: self.aggregation(self.transformation(rdd))
