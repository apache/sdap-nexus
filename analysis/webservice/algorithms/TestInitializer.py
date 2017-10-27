"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from webservice.NexusHandler import NexusHandler, nexus_initializer
from nexustiles.nexustiles import NexusTileService

@nexus_initializer
class TestInitializer:

    def __init__(self):
        pass

    def init(self, config):
        print "*** TEST INITIALIZATION ***"