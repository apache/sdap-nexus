
from tcpstream import LengthHeaderTcpProcessor, start_server


def echo(self, data):
    yield str(data)

start_server(echo, LengthHeaderTcpProcessor)
