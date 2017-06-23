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

from errors.errors import ExecutorError


class Executor:
    """
    Basic class for Executor classes
    """

    def __init__(self, input_data, ssc=None):
        """
        Create Executor and set initial data for executor
        :param input_data: Data of RDD or Dstream class
        """
        self._data = input_data
        self._ssc = ssc
        self._action = None
        self._options = None
        self._result = None

    def run_pipeline(self):
        """
        run_pipeline runs execution of pipeline actions on the data
        :return: None
        """
        print("Override me in child classes!")

    def stop_pipeline(self):
        """
        run_pipeline runs execution of pipeline actions on the data
        :return: None
        """
        print("Override me in child classes!")

    def set_pipeline_processing(self, action, options):
        """
        set_pipeline_processing sets the action and parameters that will be performed on the data
        :param action: Sequence of actions on data
        :param options: options of pipeline
        :return: None
        """
        print("Override me in child classes!")


class StreamingExecutor(Executor):
    """
    StreamingExecutor is a class for execution action with Dstream data
    """

    def run_pipeline(self):
        """
        run_pipeline runs execution of pipeline actions on the streaming data
        :return: None
        """
        if (self._action):
            self._data.foreachRDD(self._action)
        else:
            raise ExecutorError("Error: action and options don't set. Use set_pipeline_processing")
        self._ssc.start()
        self._ssc.awaitTermination()

    def set_pipeline_processing(self, action, options={}):
        """
        set_pipeline_processing sets the action and parameters that will be performed on the streaming data
        :param action: Sequence of actions on data
        :param options: options of pipeline
        :return: None
        """

        self._action = action
        self._options = options

    def stop_pipeline(self):
        self._ssc.stop(stopSparkContext=True, stopGraceFully=True)


class BatchExecutor(Executor):
    """
    BatchExecutor is a class for execution action with RDD data
    """

    def run_pipeline(self):
        """
        run_pipeline runs execution of pipeline actions on the streaming data
        :return: None
        """
        if (self._action):
            return self._action(self._data)
        else:
            raise ExecutorError("Error: action and options don't set. Use set_pipeline_processing")

    def set_pipeline_processing(self, action, options={}):
        """
        set_pipeline_processing sets the action and parameters that will be performed on the streaming data
        :param action: Sequence of actions on data
        :param options: options of pipeline
        :return: None
        """
        self._action = action
        self._options = options

    def stop_pipeline(self):
        pass
