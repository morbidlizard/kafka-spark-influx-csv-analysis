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

import geoip2.database
import os


class GeoIPReaderSingleton(object):
    __instance = None

    def __new__(cls, path):
        if cls.__instance is None:
            cls.__instance = geoip2.database.Reader(os.path.basename(path))

        return cls.__instance


def country(ip_addr, country_path):
    import geoip2.database
    country_reader = GeoIPReaderSingleton(country_path)

    try:
        match = country_reader.country(ip_addr)
        return match.country.name
    except geoip2.errors.AddressNotFoundError:
        return "unknown_country"


def city(ip_addr, city_path):
    import geoip2.database
    city_reader = GeoIPReaderSingleton(city_path)

    try:
        match = city_reader.city(ip_addr)
        return match.city.names["en"] if match.city.names else "unknown_city"
    except geoip2.errors.AddressNotFoundError:
        return "unknown_city"


def aarea(ip_addr, asn_path):
    import geoip2.database
    asn_reader = GeoIPReaderSingleton(asn_path)

    try:
        match = asn_reader.asn(ip_addr)
        return str(match.autonomous_system_number)
    except geoip2.errors.AddressNotFoundError:
        return "unknown_asn"
