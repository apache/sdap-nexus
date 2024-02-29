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


import json

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler


@nexus_handler
class HeartbeatCalcHandlerImpl(NexusCalcHandler):
    name = "Backend Services Status"
    path = "/heartbeat"
    description = "Returns health status of Nexus backend services"
    params = {}
    singleton = True

    def calc(self, computeOptions, **args):
        solrOnline = self._get_tile_service().pingSolr()

        # Not sure how to best check cassandra cluster status so just return True for now
        cassOnline = True

        if solrOnline and cassOnline:
            status = {"online": True}
        else:
            status = {"online": False}

        class SimpleResult(object):
            def __init__(self, result):
                self.result = result

            def toJson(self):
                return json.dumps(self.result)

        return SimpleResult(status)
