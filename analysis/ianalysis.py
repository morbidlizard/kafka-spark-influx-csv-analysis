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

class IAnalysis(object):
    def __init__(self, config, historical_data_repository, alert_sender, data_structure):
        self._config = config
        self._historical_data_repository = historical_data_repository
        self._alert_sender = alert_sender
        self._input_fields = dict(map(lambda x: reversed(x), enumerate(
            map(lambda x: x["input_field"], filter(lambda x: not x["key"], data_structure["rule"])))))
        self._key_fields_name = map(lambda x: x["input_field"], filter(lambda x: x["key"], data_structure["rule"]))
    def get_analysis_lambda(self):
        raise NotImplementedError("get_analysis_lambda method should be overrided!")