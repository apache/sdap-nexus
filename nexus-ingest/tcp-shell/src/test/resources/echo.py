"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from tcpstream import LengthHeaderTcpProcessor, start_server


def echo(self, data):
    yield str(data)

start_server(echo, LengthHeaderTcpProcessor)
