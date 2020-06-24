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

from webservice.NexusHandler import nexus_handler, AVAILABLE_HANDLERS
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults


@nexus_handler
class CapabilitiesListCalcHandlerImpl(NexusCalcHandler):
    name = "Capabilities"
    path = "/capabilities"
    description = "Lists the current capabilities of this Nexus system"
    params = {}
    singleton = True

    def __init__(self):
        NexusCalcHandler.__init__(self)

    def calc(self, computeOptions, **args):
        capabilities = []

        for capability in AVAILABLE_HANDLERS:
            capabilityDef = {
                "name": capability.name,
                "path": capability.path,
                "description": capability.description,
                "parameters": capability.params
            }
            capabilities.append(capabilityDef)

        return CapabilitiesResults(capabilities)


class CapabilitiesResults(NexusResults):
    def __init__(self, capabilities):
        NexusResults.__init__(self)
        self.__capabilities = capabilities

    def toJson(self):
        return json.dumps(self.__capabilities, indent=4)
