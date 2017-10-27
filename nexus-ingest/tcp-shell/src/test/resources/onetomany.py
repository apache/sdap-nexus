"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from tcpstream import LengthHeaderTcpProcessor, start_server


def echo(self, data):
    for x in xrange(0, 20):
        yield str(data) + str(x)

start_server(echo, LengthHeaderTcpProcessor)
