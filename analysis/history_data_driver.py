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
        """ Get points between (from_nanoseconds;to_nanoseconds) from measurement by tag"""
        query = "SELECT * from {0} WHERE time > {1} AND time < {2}".format(measurement, from_nanoseconds,
                                                                           to_nanoseconds)
        if tag:
            str_tags = map(lambda x: " AND \"{}\"='{}'".format(x[0], x[1]), tag.items())
            query += ''.join(list(str_tags))
        result = self.client.query(query)
        return list(result.get_points(measurement=measurement))
