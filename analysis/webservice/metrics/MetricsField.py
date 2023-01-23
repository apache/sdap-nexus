# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
