import random

from singleton import Singleton


class HistoricalDataDelivery(object):
    def __init__(self, config):
        self._config = config

    def read(self, **kwargs):
        raise NotImplementedError("read method should be overrided!")


class HistoricalDataDeliveryFactory(object):
    def __init__(self, config):
        self._config = config

    def instance_data_delivery(self):
        if self._config["historical"]["method"] == "mock":
            return MockInfluxRead(self._config["historical"])


class MockInfluxRead(HistoricalDataDelivery, metaclass=Singleton):
    def __init__(self, config):
        self._deviation = config["options"]["deviation"]

    def read(self, timestamp1, timestamp2, data, key=None):
        rnd = random.randint(0, 15)
        if rnd > 10:
            n = 2
        elif rnd > 3:
            n = 1
        else:
            n = 0
        result = []
        if isinstance(data, tuple):
            for i in range(n):
                tmp = [timestamp1 + random.randint(0, int((timestamp2 - timestamp1) * 100)) / 100]
                if key:
                    tmp.append(key)
                for j in range(len(data)):
                    value = data[j]
                    delta = int(data[j] * self._deviation / 100.)
                    if 0 == delta:
                        delta = 4
                    rnd = random.randint(0, delta) - int(delta / 2)
                    tmp.append(value + rnd)
                result.append(tuple(tmp))
        return result
