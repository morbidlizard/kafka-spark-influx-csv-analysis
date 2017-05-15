import geoip2.database

class GeoIPReaderSingleton(object):
    __instance = None
    def __new__(cls, path):
        if cls.__instance is None:
            cls.__instance =  geoip2.database.Reader(path)
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