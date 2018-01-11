"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from webservice.NexusHandler import nexus_initializer


@nexus_initializer
class TestInitializer:

    def __init__(self):
        pass

    def init(self, config):
        print "*** TEST INITIALIZATION ***"
