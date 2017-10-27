"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import time

from webservice.NexusHandler import CalcHandler
from webservice.NexusHandler import nexus_handler


@nexus_handler
class DelayHandlerImpl(CalcHandler):
    name = "Delay"
    path = "/delay"
    description = "Waits a little while"
    params = {}
    singleton = True

    def __init__(self):
        CalcHandler.__init__(self)

    def calc(self, computeOptions, **args):
        time.sleep(10)

        class SimpleResult(object):
            def toJson(self):
                return ""

        return SimpleResult()
