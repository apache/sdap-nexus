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

from collections import OrderedDict
import logging

metrics_logger = logging.getLogger(__name__)


class MetricsRecord(object):
    def __init__(self, fields):
        self._fields = OrderedDict()
        for field in fields:
            self._fields[field.key] = field

    def record_metrics(self, **kwargs):
        for field_key, addend in list(kwargs.items()):
            if field_key in self._fields:
                self._fields[field_key].add(addend)

    def print_metrics(self, logger=None, include_zero_values=False):
        if not logger:
            logger = metrics_logger

        logging_lines = []
        for field in list(self._fields.values()):
            value = field.value()
            if value > 0 or include_zero_values:
                line = "{description}: {value}".format(description=field.description, value=field.value())
                logging_lines.append(line)

        logger.info('\n'.join(logging_lines))

    def write_metrics(self):
        raise NotImplementedError
