#!/usr/bin/env pytest

# python-symmetricjsonrpc3
# Copyright (C) 2009 Egil Moeller <redhog@redhog.org>
# Copyright (C) 2009 Nicklas Lindgren <nili@gulmohar.se>
# Copyright (C) 2024 Robert "Robikz" Zalewski <zalewapl@gmail.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
# USA
import socket
import unittest

from symmetricjsonrpc3 import json
from symmetricjsonrpc3.rpc import (ClientConnection, RPCClient,
                                   RPCErrorResponse, RPCServer,
                                   dispatcher)


debug_tests = False


class _EchoDispatcher:
    def __init__(self, subject, parent):
        if not hasattr(parent, "writer"):
            parent = parent.parent
        parent.writer.write_value(subject)


class _EchoClient(ClientConnection):
    Request = _EchoDispatcher


class _ThreadedEchoClient(ClientConnection):
    class Request(dispatcher.ThreadedClient):
        Thread = _EchoDispatcher


class _EchoServer(dispatcher.ServerConnection):
    InboundConnection = _EchoClient


class _ThreadedEchoServer(dispatcher.ServerConnection):
    class InboundConnection(dispatcher.ThreadedClient):
        Thread = _ThreadedEchoClient


class _PingRPCClient(RPCClient):
    class Request(RPCClient.Request):
        def dispatch_request(self, subject):
            if debug_tests:
                print("PingClient: dispatch_request", subject)
            assert subject['method'] == "pingping"
            return "pingpong"


class _PongRPCServer(RPCServer):
    class InboundConnection(RPCServer.InboundConnection):
        class Thread(RPCServer.InboundConnection.Thread):
            class Request(RPCServer.InboundConnection.Thread.Request):
                def dispatch_request(self, subject):
                    if debug_tests:
                        print("TestPongRPCServer: dispatch_request", subject)
                    assert subject['method'] == "ping"
                    assert self.parent.request("pingping", wait_for_response=True) == "pingpong"
                    if debug_tests:
                        print("TestPongRPCServer: back-pong")
                    return "pong"


def _make_server_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('localhost', 0))
    s.listen(1)
    return s, s.getsockname()[1]


def _make_client_socket(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', port))
    return s


class TestRPCErrorResponse(unittest.TestCase):
    def test_init(self):
        response = RPCErrorResponse("TestMessage", 1337, data=[1, 3, 6])
        self.assertEqual(response["message"], "TestMessage")
        self.assertEqual(response["code"], 1337)
        self.assertEqual(response["data"], [1, 3, 6])

    def test_exception_conversion(self):
        exception = Exception("I am error.")
        response = RPCErrorResponse(exception)
        self.assertEqual(response["message"], "Exception: I am error.")
        self.assertEqual(response["code"], 0)

        data = response["data"]
        self.assertEqual(data, {
            **data,
            "type": "Exception",
            "args": ["I am error."]
        }, data)
        self.assertIs(exception, data.get("exception"))

    def test_exception_conversion_for_sending(self):
        exception = Exception("I am error.")
        response = RPCErrorResponse(exception, for_sending=True)
        self.assertEqual(response["message"], "Exception: I am error.")
        self.assertEqual(response["code"], 0)

        data = response["data"]
        self.assertEqual(data, {
            "type": "Exception",
            "args": ["I am error."]
        }, data)
        self.assertNotIn("exception", data)

    def test_tojson_error_conversion(self):
        class SomeObj:
            def __init__(self, ident):
                self.ident = ident

            def __to_json__(self):
                return {"SomeObj": self.ident}

            def __repr__(self):
                return repr(str(self))

            def __str__(self):
                return f"I am SomeObj {self.ident}."

        exception = Exception(SomeObj("A"), SomeObj("B"))
        response = RPCErrorResponse(exception)

        self.assertEqual(response["message"],
                         "Exception: ('I am SomeObj A.', 'I am SomeObj B.')")
        self.assertEqual(response["code"], 0)

        data = response["data"]
        self.assertEqual(data, {
            **data,
            "type": "Exception",
            "args": [SomeObj('A').__to_json__(), SomeObj('B').__to_json__()]
        })
        self.assertIs(exception, data["exception"])


class TestRPC(unittest.TestCase):
    def test_client(self):
        s1, s2 = socket.socketpair()
        try:
            _EchoClient(s2)
        except Exception:
            s1.close()
            s2.close()
            raise

        with SocketFile(s1) as s1:
            reader = json.Reader(s1)
            writer = json.Writer(s1)

            obj = {'foo': 1, 'bar': [1, 2]}
            writer.write_value(obj)
            return_obj = reader.read_value()
        self.assertEqual(obj, return_obj)

    def test_return_on_closed_socket(self):
        server_socket, port = _make_server_socket()
        echo_server = _EchoServer(server_socket, name="TestEchoServer")

        with SocketFile(_make_client_socket(port)) as client_socket:
            writer = json.Writer(client_socket)
            writer.write_value({'foo': 1, 'bar': 2})

        echo_server.shutdown()
        echo_server.join()

    def test_server(self):
        for n in range(3):
            server_socket, port = _make_server_socket()
            echo_server = _EchoServer(server_socket, name="TestEchoServer")

            with SocketFile(_make_client_socket(port)) as client_socket:
                reader = json.Reader(client_socket)
                writer = json.Writer(client_socket)

                obj = {'foo': 1, 'bar': [1, 2]}
                writer.write_value(obj)
                return_obj = reader.read_value()

            self.assertEqual(obj, return_obj)
            echo_server.shutdown()
            echo_server.join()

    def test_threaded_server(self):
        for n in range(3):
            server_socket, port = _make_server_socket()
            echo_server = _ThreadedEchoServer(server_socket, name="TestEchoServer")

            with SocketFile(_make_client_socket(port)) as client_socket:
                writer = json.Writer(client_socket)

                obj = {'foo': 1, 'bar': [1, 2]}
                writer.write_value(obj)

                reader = json.Reader(client_socket)
                return_obj = reader.read_value()

            self.assertEqual(obj, return_obj)
            echo_server.shutdown()
            echo_server.join()

    def test_rpc_server(self):
        for n in range(3):
            server_socket, port = _make_server_socket()
            server = _PongRPCServer(server_socket, name="PongServer")

            client_socket = _make_client_socket(port)
            client = _PingRPCClient(client_socket)
            self.assertEqual(client.request("ping", wait_for_response=True), "pong")
            self.assertEqual(client.ping(), "pong")
            client.shutdown()
            server.shutdown()
            server.join()


class SocketFile:
    def __init__(self, sock):
        self.s = sock

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.s.close()

    def fileno(self):
        return self.s.fileno()

    def read(self, n=1024):
        return self.s.recv(1024).decode('ascii')

    def write(self, data):
        return self.s.send(data.encode('ascii'))

    def flush(self):
        pass

    def close(self):
        return self.s.close()


if __name__ == "__main__":
    unittest.main()
