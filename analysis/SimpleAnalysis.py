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


class SimpleAnalysis(IUserAnalysis):
    def __init__(self, option, name):
        super().__init__(option, name)
        self._deviations = option["deviation"]
        self._batch_number = option["batch_number"]

    def analysis(self, historical_data, alert_sender):

        for field in self._deviations.keys():
            hist_val_vec = historical_data[self._batch_number]
            if hist_val_vec:
                lower_bound = hist_val_vec[field] * (1 - float(self._deviations[field]) / 100)
                upper_bound = hist_val_vec[field] * (1 + float(self._deviations[field]) / 100)

                current_value_vec = historical_data[0]
                if (current_value_vec[field] < lower_bound) or (current_value_vec[field] > upper_bound):
                    alert_sender.send_message(AnalysisModule=self.name, timestamp=time(),
                                              param={"key": current_value_vec["key"],
                                                     "field": field,
                                                     "lower_bound": lower_bound,
                                                     "upper_bound": upper_bound,
                                                     "value": current_value_vec[field]})
