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

from time import time
from analysis.iuseranalysis import IUserAnalysis


class AverageAnalysis(IUserAnalysis):
    def __init__(self, option, name):
        super().__init__(option, name)
        self._deviations = option["deviation"]
        self._num_average = option["num_average"]

    def analysis(self, historical_data, alert_sender):
        historical_data_vectors = []
        for i in range(self._num_average):
            historical_data_vectors.append(historical_data[1 + i])

        for field in self._deviations.keys():
            count_vec = 0
            sum_field = 0
            for historical_data_vector in historical_data_vectors:
                if historical_data_vector:
                    sum_field += historical_data_vector[field]
                    count_vec += 1
            if count_vec:
                average = sum_field / count_vec
            else:
                average = 0
            if average:
                lower_bound = average * (1 - float(self._deviations[field]) / 100)
                upper_bound = average * (1 + float(self._deviations[field]) / 100)

                current_value = historical_data[0][field]
                if (current_value < lower_bound) or (current_value > upper_bound):
                    alert_sender.send_message(AnalysisModule=self.name, timestamp=time(),
                                              param={"key": historical_data[0]["key"],
                                                     "field": field,
                                                     "lower_bound": lower_bound,
                                                     "upper_bound": upper_bound,
                                                     "value": current_value})
