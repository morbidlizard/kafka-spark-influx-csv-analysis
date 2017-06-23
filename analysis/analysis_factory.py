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

from importlib import import_module

from kafka import KafkaProducer

from analysis.alert_message import AlertMessageFactory
from analysis.historical_delivery import HistoricalDataDeliveryFactory
from analysis.historical_data import HistoricalData
from pyspark import RDD


class AnalysisFactory(object):
    """
        Main entry point to analysis module. A AnalysisFactory create instances depending on the configuration 
        and return lambda function.
    """

    def __init__(self, config, data_structure_after_aggregation, enumerate_output_aggregation_field):
        """
        :param config: dictionary with configuration parameters. 
        :param data_structure_after_aggregation:  Structure of the data after the aggregation operation. The 
        structure include fields name, the key flag and the name of function
        :param enumerate_output_aggregation_field:  Dictionary of the data after the aggregation operation exclude key 
        field. The dictionary include the name of field and index in tuple after aggregation
        """
        self._historical_data_repository = HistoricalDataDeliveryFactory(
            config.content["analysis"]).instance_data_delivery()
        self._config = config.content
        self._data_structure_after_aggregation = data_structure_after_aggregation
        self._input_fields = enumerate_output_aggregation_field
        self._alert_sender = AlertMessageFactory(config.content).instance_alert()
        self._accuracy = self._config["analysis"]["accuracy"]
        self._batch_duration = self._config["input"]["options"]["batchDuration"]
        self._key_fields_name = list(map(lambda x: x["input_field"],
                                         filter(lambda x: x["key"], data_structure_after_aggregation["rule"])))

    def get_analysis_lambda(self):
        """
        Creates a lambda function that analyzes data after aggregation
        :return: lambda function under tuple or rdd object 
        """

        alert_sender_singleton = self._alert_sender
        historical_data_repository_singleton = self._historical_data_repository
        measurement = self._config["analysis"]["historical"]["influx_options"]["measurement"]
        user_analysis_module = self._config["analysis"]["rule"]

        user_analysis = []
        missing_dependencies = []

        for user_module in user_analysis_module:
            try:
                user_analysis.append(
                    {"module": user_module["module"], "name": user_module["name"],
                     "import_module": import_module("analysis." + user_module["module"]),
                     "options": user_module["options"]})
            except ImportError as e:
                missing_dependencies.append(user_module)

        if missing_dependencies:
            raise ImportError("Missing required analysis module {0}".format(missing_dependencies))

        user_object_analysis = list(map(lambda x: getattr(x["import_module"], x["module"])(x["options"], x["name"]),
                                        user_analysis))

        historical_data = HistoricalData(historical_data_repository_singleton, self._input_fields, measurement,
                                         self._accuracy, self._key_fields_name, self._batch_duration)

        def run_necessary_lambda(rdd_or_object):
            if isinstance(rdd_or_object, RDD):
                rdd_or_object.foreachPartition(
                    lambda iterator: map(
                        lambda record: analysis_record(record[1:], user_object_analysis,
                                                       alert_sender_singleton, historical_data, record[0]), iterator))
            else:
                analysis_record(rdd_or_object, user_object_analysis,
                                alert_sender_singleton, historical_data)

        return lambda rdd_or_object: run_necessary_lambda(rdd_or_object)


def analysis_record(input_tuple_value, user_object_analysys, alert_sender_singleton, historical_data, key=None):
    """
    Checks data on anomalies using user-defined classes
    :param input_tuple_value: input data to check
    :param user_object_analysys: object of user-defined classes
    :param alert_sender_singleton: object that send alert
    :param historical_data: Object for obtaining historical data
    :param key: This is the key to the data aggregation
    :return: 
    """
    historical_data.set_zero_value(input_tuple_value)
    historical_data.set_key(key)
    for object_analysis in user_object_analysys:
        object_analysis.analysis(historical_data, alert_sender_singleton)
