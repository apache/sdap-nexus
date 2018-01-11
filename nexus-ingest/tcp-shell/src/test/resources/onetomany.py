
from tcpstream import LengthHeaderTcpProcessor, start_server


def echo(self, data):
    for x in xrange(0, 20):
        yield str(data) + str(x)

start_server(echo, LengthHeaderTcpProcessor)
