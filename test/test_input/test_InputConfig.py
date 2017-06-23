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
import unittest

from input.input_module import InputConfig


class InputConfigTestCase(unittest.TestCase):
    def test__init__(self):
        config = InputConfig(os.path.join(os.path.dirname(__file__), os.path.join("..", "data", "input_config_csv.json")))
        self.assertIsInstance(config, InputConfig, "Config should be instance InputConfig")
