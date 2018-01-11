
from tcpstream import LengthHeaderTcpProcessor, start_server
from os import environ

def echo(self, data):
    env = environ['VAR']
    yield "%s %s" % (env, data)

start_server(echo, LengthHeaderTcpProcessor)
