from time import time
from analysis.iuseranalysis import IUserAnalysis


class AverageAnalysis(IUserAnalysis):
    def __init__(self, option):
        super().__init__(option)
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
                    alert_sender.send_message(AnalysisModule="AverageAnalysis", timestamp=time(),
                                              param={"key": historical_data[0]["key"],
                                                     "field": field,
                                                     "lower_bound": lower_bound,
                                                     "upper_bound": upper_bound,
                                                     "value": current_value})
