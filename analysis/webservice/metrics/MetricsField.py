from abc import abstractmethod


class MetricsField(object):
    def __init__(self, key, description, initial_value):
        self.key = key
        self.description = description
        self._value = initial_value

    @abstractmethod
    def add(self, addend):
        pass

    def value(self):
        return self._value


class SparkAccumulatorMetricsField(MetricsField):
    def __init__(self, key, description, accumulator):
        super(SparkAccumulatorMetricsField, self).__init__(key, description, accumulator)

    def add(self, addend):
        self._value.add(addend)

    def value(self):
        return self._value.value


class NumberMetricsField(MetricsField):
    def __init__(self, key, description, initial_value=0):
        super(NumberMetricsField, self).__init__(key, description, initial_value)

    def add(self, addend):
        self._value += addend
