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

from datetime import datetime

from pytz import timezone

import BaseDomsHandler
import datafetch
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler as BaseHandler
from webservice.NexusHandler import nexus_handler

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))


@nexus_handler
class DomsValuesQueryHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "DOMS In-Situ Value Lookup"
    path = "/domsvalues"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):
        source = computeOptions.get_argument("source", None)
        startTime = computeOptions.get_start_datetime()
        endTime = computeOptions.get_end_datetime()
        bbox = computeOptions.get_argument("b", None)
        timeTolerance = computeOptions.get_float_arg("tt")
        depth_min = computeOptions.get_float_arg("depthMin", default=None)
        depth_max = computeOptions.get_float_arg("depthMax", default=None)
        radiusTolerance = computeOptions.get_float_arg("rt")
        platforms = computeOptions.get_argument("platforms", "")

        source1 = self.getDataSourceByName(source)
        if source1 is None:
            raise Exception("Source '%s' not found" % source)

        values, bounds = datafetch.getValues(source1, startTime.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                             endTime.strftime('%Y-%m-%dT%H:%M:%SZ'), bbox, depth_min, depth_max,
                                             platforms, placeholders=True)
        count = len(values)

        args = {
            "source": source,
            "startTime": startTime,
            "endTime": endTime,
            "bbox": bbox,
            "timeTolerance": timeTolerance,
            "depthMin": depth_min,
            "depthMax": depth_max,
            "radiusTolerance": radiusTolerance,
            "platforms": platforms
        }

        return BaseDomsHandler.DomsQueryResults(results=values, args=args, bounds=bounds, details={}, count=count,
                                                computeOptions=None)
