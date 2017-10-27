"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import socket
from os import environ
from struct import unpack, pack

__author__ = 'Frank Greguska'
'''
Provides a class that can accept data from a TCP Connection and act as a Spring XD processor.
Implementors should call the start_server method, passing in a function that will be invoked
to handle data and a ProcessorType.
'''

RECEIVE_BUFFER_SIZE = 4096


def start_server(handler_method, processor_type):
    Handler = type('Handler', (processor_type, object), {'generate_responses': handler_method})

    try:
        outbound_port = int(environ['OUTBOUND_PORT'])
    except KeyError as e:
        raise EnvironmentError("Environment variable %s is required" % e.args[0])

    try:
        inbound_port = int(environ['INBOUND_PORT'])
    except KeyError as e:
        raise EnvironmentError("Environment variable %s is required" % e.args[0])

    inbound_socket = open_port(inbound_port)
    outbound_socket = open_port(outbound_port)

    inbound_socket, client_address = inbound_socket.accept()
    print "Connected client %s" % str(client_address)
    outbound_socket, client_address = outbound_socket.accept()
    print "Connected client %s" % str(client_address)

    processor = Handler(inbound_socket, outbound_socket)

    try:
        while True:
            processor.handle()
    finally:
        try:
            inbound_socket.close()
        finally:
            outbound_socket.close()


def open_port(port_number):
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.bind(('127.0.0.1', port_number))

    data_socket.listen(0)
    print "Listening on %s" % str(data_socket.getsockname())

    wait_for_test_connection(data_socket)

    return data_socket


# We expect one initial test connection to be made when the script is first started
# Discard this first connection
def wait_for_test_connection(data_socket):
    data_socket, client_address = data_socket.accept()
    print "Test connection successful %s" % str(client_address)
    data_socket.close()


class LengthHeaderTcpProcessor(object):
    """ProcessorType that expects a 4 byte message header containing the total data length."""

    HEADER_SIZE = 4

    def __init__(self, inbound_socket, outbound_socket):
        self.inbound_socket = inbound_socket
        self.outbound_socket = outbound_socket

    def read_header(self, data):
        bytestring = data[:LengthHeaderTcpProcessor.HEADER_SIZE]
        message_length = unpack("!L", bytestring)[0]
        return message_length

    def decode(self, data):
        data = data[LengthHeaderTcpProcessor.HEADER_SIZE:]
        return data

    def encode(self, data):
        encoded = pack("!L", len(data)) + data
        return encoded

    def handle(self):
        # self.inbound_socket is the socket used to accept incoming messages
        in_data = self.inbound_socket.recv(RECEIVE_BUFFER_SIZE)

        # Discard anything that doesn't have an appropriate header size
        if len(in_data) >= LengthHeaderTcpProcessor.HEADER_SIZE:

            # Read the length of the message
            message_length = self.read_header(in_data)

            # Continue reading data until the length has been reached
            while len(in_data) - LengthHeaderTcpProcessor.HEADER_SIZE < message_length:
                in_data += self.inbound_socket.recv(RECEIVE_BUFFER_SIZE)

            in_data = self.decode(in_data)

            try:
                assert len(in_data) == message_length
            except AssertionError:
                raise AssertionError(
                    "Expected data length %s but found data of length %s. Data received %s" % (
                    message_length, len(in_data), str(in_data)))

            # self.outbound_socket is the socket used to send messages back to the client
            for out_data in self.generate_responses(in_data):
                self.outbound_socket.sendall(self.encode(out_data))

            # once the generator is exhausted, acknowledge the incoming message to signal we are
            # ready to process more data
            self.inbound_socket.sendall(self.encode("ACK"))
        elif len(in_data) == 0:
            raise RuntimeError("Client closed connection.")
        else:
            print "Received unknown data: %s %s" % (len(in_data), in_data)

    def generate_responses(self, data):
        raise NotImplementedError("Instances of LengthHeaderTcpProcessor should implement this method.")


def echo(self, data):
    return data


def repeat(self, data):
    for _ in xrange(0, 5):
        yield data


if __name__ == "__main__":
    start_server(repeat, LengthHeaderTcpProcessor)
