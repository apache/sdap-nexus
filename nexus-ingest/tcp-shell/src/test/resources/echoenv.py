"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from tcpstream import LengthHeaderTcpProcessor, start_server
from os import environ

def echo(self, data):
    env = environ['VAR']
    yield "%s %s" % (env, data)

start_server(echo, LengthHeaderTcpProcessor)
