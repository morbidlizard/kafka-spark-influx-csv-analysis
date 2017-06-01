from time import time
from analysis.iuseranalysis import IUserAnalysis


class SimplyAnalysis(IUserAnalysis):
    def __init__(self, option):
        super().__init__(option)
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
                    alert_sender.send_message(AnalysisModule="SimplyAnalysis", timestamp=time(),
                                              param={"key": current_value_vec["key"],
                                                     "field": field,
                                                     "lower_bound": lower_bound,
                                                     "upper_bound": upper_bound,
                                                     "value": current_value_vec[field]})
