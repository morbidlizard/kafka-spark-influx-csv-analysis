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

class HistoryDataSingleton:
    __instance = None

    def __new__(cls, client):
        if cls.__instance is None:
            cls.__instance = HistoryDataDriver(client)
        return cls.__instance


class HistoryDataDriver:
    def __init__(self, client):
        self.client = client

    def read(self, measurement, from_nanoseconds, to_nanoseconds, tag=None):
        query = "SELECT * from {0} WHERE time > {1} AND time < {2}".format(measurement, from_nanoseconds,
                                                                           to_nanoseconds)
        if tag:
            str_tags = map(lambda x: " AND \"{}\"='{}'".format(x[0], x[1]), tag.items())
            query += ''.join(list(str_tags))
        result = self.client.query(query)
        return list(result.get_points(measurement=measurement))
