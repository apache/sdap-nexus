"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from webservice.NexusHandler import CalcHandler, nexus_handler


@nexus_handler
class ErrorTosserHandler(CalcHandler):
    name = "MakeError"
    path = "/makeerror"
    description = "Causes an error"
    params = {}
    singleton = True

    def __init__(self):
        CalcHandler.__init__(self)

    def calc(self, computeOptions, **args):
        a = 100 / 0.0
        # raise Exception("I'm Mad!")
        # raise NexusProcessingException.NexusProcessingException(NexusProcessingException.StandardNexusErrors.UNKNOWN, "I'm Mad!")
        return {}, None, None
