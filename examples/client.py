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

COMM = -1
INFO = 0
DEBUG = 1


def log(level, *args, **kwargs):
    if g_loglevel >= level:
        print(*args, **kwargs)


class PingRPCClient(symmetricjsonrpc3.RPCClient):
    class Request(symmetricjsonrpc3.RPCClient.Request):
        def dispatch_request(self, subject):
            # Handle callbacks from the server
            log(COMM, f"-> REQ: dispatch_request({repr(subject)})")
            assert subject['method'] == "pingping"
            return "pingpong"


def parse_args():
    global g_loglevel

    argp = argparse.ArgumentParser()
    argp.add_argument("-H", "--host", default="localhost",
                      help="host to connect to [%(default)s]")
    argp.add_argument("-p", "--port", default=4712,
                      help="port to connect to [%(default)s]")
    argp.add_argument("-q", "--quiet", default=0, action="count",
                      help="decrease verbosity level")
    argp.add_argument("-v", "--verbose", default=0, action="count",
                      help="increase verbosity level")
    argp.add_argument("--ssl", action="store_true", help=(
        "Encrypt communication with SSL using M2Crypto. "
        "Requires a server.pem in the current directory."))

    args = argp.parse_args()
    g_loglevel = args.verbose - args.quiet
    return args


args = parse_args()

if args.ssl:
    # Set up an SSL connection
    import M2Crypto
    ctx = M2Crypto.SSL.Context()
    ctx.set_verify(M2Crypto.SSL.verify_peer | M2Crypto.SSL.verify_fail_if_no_peer_cert, depth=9)
    if ctx.load_verify_locations('server.pem') != 1:
        raise Exception('No CA certs')
    s = M2Crypto.SSL.Connection(ctx)
else:
    # Set up a TCP socket
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#  Connect to the server
log(INFO, f"Connecting to ({args.host}:{args.port}) ...")
s.connect((args.host, args.port))
log(DEBUG, f"Connected to ({args.host}:{args.port})")

# Create a client thread handling for incoming requests
log(DEBUG, "Creating Pong client ...")
client = PingRPCClient(s)

# Call a method on the server
log(INFO, "Sending 'ping' request ...")
res = client.request("ping", wait_for_response=True)
log(COMM, f"-> RES: client.ping => {repr(res)}")
assert res == "pong"

# Notify server it can shut down
log(DEBUG, "Telling server to shut down ...")
client.notify("shutdown")

log(DEBUG, "Shutting down ourselves ...")
client.shutdown()

log(INFO, "Done!")
