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

import json
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock
from analysis.alert_message import KafkaOutAlert


class TestConfig():
    def __init__(self, input_content):
        self.content = input_content


class TestKafkaOutAlert(TestCase):
    @patch('analysis.alert_message.SingleKafkaProducer', autospec=True)
    def test_send_message(self, mock_single_kafka_producer):
        mock_class = MagicMock()
        mock_class.send.return_value = None
        mock_class.flush.return_value = None
        mock_single_kafka_producer.return_value = mock_class

        config = TestConfig(
            {
                "input": {
                    "options": {
                        "batchDuration": 4
                    }
                },
                "analysis": {
                    "historical": {
                        "method": "influx",
                        "influx_options": {
                            "measurement": "mock"
                        }
                    },
                    "alert": {
                        "method": "kafka",
                        "option": {
                            "server": "localhost",
                            "port": 29092,
                            "topic": "testalert"
                        }
                    },
                    "accuracy": 3,
                    "rule": {
                        "MockAnalysis": {
                            "deviation": {
                                "packet_size": 5,
                                "traffic": 8
                            },
                            "batch_number": 1
                        }
                    }
                }
            })

        test_alert_kafka = KafkaOutAlert(config.content)
        test_alert_kafka.send_message(va1_x="test_val", val_y={"x": 1, "y": 2})
        self.assertTrue(mock_class.send.called,
                        "Failed. The send didn't call in send_message method.")
        mock_class.send.assert_called_with('testalert',
            str.encode(json.dumps({"va1_x": "test_val", "val_y": {"x": 1, "y": 2}})))

        self.assertTrue(mock_class.flush.called,
                        "Failed. The send didn't call in send_message method.")
