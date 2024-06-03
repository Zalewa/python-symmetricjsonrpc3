#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=UTF-8 :

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
import argparse
import symmetricjsonrpc3


g_loglevel = 0

ERROR = (-2, "E")
COMM = (-1, "C")
INFO = (0, "I")
DEBUG = (1, "D")


def log(level, *args, **kwargs):
    if g_loglevel >= level[0]:
        print(f"{level[1]}:", *args, **kwargs)


class PongRPCServer(symmetricjsonrpc3.RPCServer):
    class InboundConnection(symmetricjsonrpc3.RPCServer.InboundConnection):
        class Thread(symmetricjsonrpc3.RPCServer.InboundConnection.Thread):
            class Request(symmetricjsonrpc3.RPCServer.InboundConnection.Thread.Request):
                def dispatch_notification(self, subject):
                    log(COMM, f"-> NOT: dispatch_notification({repr(subject)})")
                    if subject['method'] == "shutdown":
                        # Shutdown the server. Note: We must use a
                        # notification, not a method for this - when the
                        # server's dead, there's no way to inform the
                        # client that it is...
                        self.parent.parent.parent.shutdown()
                    else:
                        log(ERROR, f"-> NOT: unexpected method: {subject['method']}")

                def dispatch_request(self, subject):
                    log(COMM, f"-> REQ: dispatch_request({repr(subject)})")
                    if subject['method'] == "ping":
                        # Call the client back
                        # self.parent is a symmetricjsonrpc3.RPCClient subclass
                        res = self.parent.request("pingping", wait_for_response=True)
                        log(COMM, f"-> RES: parent.pingping => {repr(res)}")
                        if res != "pingpong":
                            log(ERROR, f"-> RES: unexpected 'pingping' response => {repr(res)}")
                        return "pong"
                    else:
                        log(ERROR, f"-> REQ: unexpected method: {repr(subject)}")
                        # A well-behaved server would send an error response here.
                        return None


def parse_args():
    global g_loglevel

    argp = argparse.ArgumentParser()
    argp.add_argument("-H", "--host", default="localhost",
                      help="hostname to listen on [%(default)s]")
    argp.add_argument("-p", "--port", default=4712,
                      help="port to listen on [%(default)s]")
    argp.add_argument("-q", "--quiet", default=0, action="count",
                      help="decrease verbosity level")
    argp.add_argument("-v", "--verbose", default=0, action="count",
                      help="increase verbosity level")
    argp.add_argument("--ssl", action="store_true", help=(
        "Encrypt communication with SSL using M2Crypto. "
        "Requires a server.pem and server.key in the current directory."))

    args = argp.parse_args()
    g_loglevel = args.verbose - args.quiet
    return args


args = parse_args()
if args.ssl:
    # Set up a SSL socket
    import M2Crypto
    ctx = M2Crypto.SSL.Context()
    ctx.load_cert('server.pem', 'server.key')
    s = M2Crypto.SSL.Connection(ctx)
else:
    # Set up a TCP socket
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

#  Start listening on the socket for connections
log(DEBUG, f"Binding server socket to ({args.host}:{args.port}) ...")
s.bind((args.host, args.port))
s.listen(1)
log(INFO, f"Listening on ({args.host}:{args.port}) ...")

# Create a server thread handling incoming connections
log(DEBUG, "Creating Pong server ...")
server = PongRPCServer(s, name="PongServer")

try:
    log(INFO, "Serving clients ...")
    # Wait for the server to stop serving clients
    server.join()
except KeyboardInterrupt:
    log(INFO, "Shutting down the server ...")
    server.shutdown()
    log(DEBUG, "Awaiting server shutdown ...")
    server.join()
log(INFO, "Done!")
