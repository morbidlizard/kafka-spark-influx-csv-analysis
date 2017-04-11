from error import ExecutorError


class Executor:
    """
    Basic class for Executor classes
    """

    def __init__(self, input_data):
        """
        Create Executor and set initial data for executor
        :param input_data: Data of RDD or Dstream class
        """
        self._data = input_data
        self._action = None
        self._options = None
        self._result = None

    def run_pipeline(self):
        """
        run_pipeline runs execution of pipeline actions on the data
        :return: None
        """

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

    def set_pipeline_processing(self, action, options={}):
        """
        set_pipeline_processing sets the action and parameters that will be performed on the streaming data
        :param action: Sequence of actions on data
        :param options: options of pipeline
        :return: None
        """

        self._action = action
        self._options = options


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
