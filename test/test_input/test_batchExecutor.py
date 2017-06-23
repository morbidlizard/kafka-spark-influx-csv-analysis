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

import os
from unittest import TestCase

from pyspark.sql import SparkSession

from errors.errors import ExecutorError
from input.executors import BatchExecutor

INPUT_PATH = os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "test.csv"))

class TestBatchExecutor(TestCase):
    def test___init__(self):
        spark = SparkSession.builder.getOrCreate()
        rdd = spark.read.csv(INPUT_PATH).rdd
        test_executor = BatchExecutor(rdd)

        self.assertIsInstance(test_executor, BatchExecutor,
                              "test_executor should be instance of BatchExecutor")

    def test_set_pipeline_processing(self):

        spark = SparkSession.builder.getOrCreate()
        rdd = spark.read.csv(INPUT_PATH).rdd
        test_executor = BatchExecutor(rdd)
        test_executor.set_pipeline_processing(lambda x: x.count())

        self.assertTrue(test_executor._action, "action should be set in set_pipeline_processing")

    def test_run_pipeline_processing(self):

        spark = SparkSession.builder.getOrCreate()
        rdd = spark.read.csv(INPUT_PATH).rdd
        test_executor = BatchExecutor(rdd)
        test_executor.set_pipeline_processing(lambda x: x.count())
        number_record = test_executor.run_pipeline()

        self.assertEqual(number_record, 5, "Result should be equal 5 (number of record in test file)")

        test_executor.set_pipeline_processing(None)

        with self.assertRaises(ExecutorError) as context:
            number_record = test_executor.run_pipeline()

        self.assertTrue("action and options" in context.exception.args[0],
                        "Catch exception, but it differs from test exception")

