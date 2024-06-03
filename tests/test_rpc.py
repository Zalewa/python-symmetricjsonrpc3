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
import time
import unittest

from symmetricjsonrpc3 import json
from symmetricjsonrpc3.rpc import (ClientConnection, RPCClient, RPCP2PNode,
                                   RPCServer,
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


class _PongRPCP2PServer(RPCP2PNode):
    class Thread(RPCP2PNode.Thread):
        class InboundConnection(RPCP2PNode.Thread.InboundConnection):
            class Thread(RPCP2PNode.Thread.InboundConnection.Thread):
                class Request(RPCP2PNode.Thread.InboundConnection.Thread.Request):
                    def dispatch_request(self, subject):
                        if debug_tests:
                            print("TestPongRPCP2PServer: dispatch_request", subject)
                        if subject['method'] == "ping":
                            assert self.parent.request("pingping", wait_for_response=True) \
                                == "pingpong"
                            if debug_tests:
                                print("TestPongRPCServer: back-pong")
                            return "pong"
                        elif subject['method'] == "pingping":
                            if debug_tests:
                                print("PingClient: dispatch_request", subject)
                            return "pingpong"
                        else:
                            assert False

        def run_parent(self):
            client = self.InboundConnection.Thread(_make_client_socket())
            self.parent.parent['result'] = client.request("ping", wait_for_response=True) == "pong"


def _make_server_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', 4712))
    s.listen(1)
    return s


def _make_client_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 4712))
    return s


class TestRpc(unittest.TestCase):
    def test_client(self):
        sockets = socket.socketpair()
        _EchoClient(sockets[1])

        reader = json.Reader(sockets[0])
        writer = json.Writer(sockets[0])

        obj = {'foo': 1, 'bar': [1, 2]}
        writer.write_value(obj)
        return_obj = reader.read_value()
        self.assertEqual(obj, return_obj)

    def test_return_on_closed_socket(self):
        server_socket = _make_server_socket()
        echo_server = _EchoServer(server_socket, name="TestEchoServer")

        client_socket = _make_client_socket()
        writer = json.Writer(client_socket)
        writer.write_value({'foo': 1, 'bar': 2})
        client_socket.close()

        echo_server.shutdown()
        echo_server.join()

    def test_server(self):
        for n in range(3):
            server_socket = _make_server_socket()
            echo_server = _EchoServer(server_socket, name="TestEchoServer")

            client_socket = _make_client_socket()
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
            server_socket = _make_server_socket()
            echo_server = _ThreadedEchoServer(server_socket, name="TestEchoServer")

            client_socket = _make_client_socket()
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
            server_socket = _make_server_socket()
            server = _PongRPCServer(server_socket, name="PongServer")

            client_socket = _make_client_socket()
            client = _PingRPCClient(client_socket)
            self.assertEqual(client.request("ping", wait_for_response=True), "pong")
            self.assertEqual(client.ping(), "pong")
            server.shutdown()
            server.join()

    def test_rpc_p2p_server(self):
        for n in range(3):
            server_socket = _make_server_socket()
            res = {}
            server = _PongRPCP2PServer(server_socket, res, name="PongServer")
            for x in range(0, 4):
                if 'result' in res:
                    break
                time.sleep(1)
            assert 'result' in res and res['result']

            server.shutdown()
            server.join()


if __name__ == "__main__":
    unittest.main()
