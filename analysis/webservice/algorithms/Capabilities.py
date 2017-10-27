"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import json

from webservice.NexusHandler import CalcHandler, nexus_handler, AVAILABLE_HANDLERS
from webservice.webmodel import NexusResults


@nexus_handler
class CapabilitiesListHandlerImpl(CalcHandler):
    name = "Capabilities"
    path = "/capabilities"
    description = "Lists the current capabilities of this Nexus system"
    params = {}
    singleton = True

    def __init__(self):
        CalcHandler.__init__(self)

    def calc(self, computeOptions, **args):
        capabilities = []

        for capability in AVAILABLE_HANDLERS:
            capabilityDef = {
                "name": capability.name(),
                "path": capability.path(),
                "description": capability.description(),
                "parameters": capability.params()
            }
            capabilities.append(capabilityDef)

        return CapabilitiesResults(capabilities)


class CapabilitiesResults(NexusResults):
    def __init__(self, capabilities):
        NexusResults.__init__(self)
        self.__capabilities = capabilities

    def toJson(self):
        return json.dumps(self.__capabilities, indent=4)
