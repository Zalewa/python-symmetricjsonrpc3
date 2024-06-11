#!/usr/bin/env python3

# python-symmetricjsonrpc3
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
import socket
from time import perf_counter, sleep

import symmetricjsonrpc3


g_loglevel = 0

ERROR = (-2, "E")
COMM = (-1, "C")
INFO = (0, "I")
DEBUG = (1, "D")


def log(level, *args, **kwargs):
    if g_loglevel >= level[0]:
        print(f"{level[1]}:", *args, **kwargs)


def avg(l):
    return sum(l) / len(l)


def ms(seconds):
    return f"{round(seconds * 1000, 3):0.3f} ms"


class PingRPCClient(symmetricjsonrpc3.RPCClient):
    class Request(symmetricjsonrpc3.RPCClient.Request):
        def dispatch_request(self, subject):
            # Handle callbacks from the server
            if subject['method'] == "pingping":
                return "pingpong"
            else:
                log(ERROR, f"-> REQ: unexpected method {subject['method']}")
                return None


def parse_args():
    global g_loglevel

    argp = argparse.ArgumentParser(
        description=("Ping spammer client; attacks ping_server.py "
                     "with ping requests."))
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

    argp.add_argument(
        "-c", "--count", default=0, type=int,
        help="amount of batches to send [infinite]")
    argp.add_argument(
        "-i", "--interval", default=0.0, type=float,
        help="seconds between ping batches (float) [%(default)s]")
    argp.add_argument(
        "-s", "--size", default=1, type=int,
        help="size of the batch [%(default)s]")

    args = argp.parse_args()
    g_loglevel = args.verbose - args.quiet
    args.count = max(0, args.count)
    args.interval = max(0, args.interval)
    args.size = max(1, args.size)
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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#  Connect to the server
log(INFO, f"Connecting to ({args.host}:{args.port}) ...")
s.connect((args.host, args.port))
log(DEBUG, f"Connected to ({args.host}:{args.port})")

# Create a client thread handling for incoming requests
log(DEBUG, "Creating Ping client ...")
client = PingRPCClient(s)

# Run the pinger.
total_ping_times = []
try:
    n_batch = 1
    while args.count <= 0 or n_batch <= args.count:
        batch_label = f"{n_batch: >3d}: " if args.count > 1 else ""
        log(DEBUG, f"{batch_label}Sending 'ping' request ...")
        n_request = 0
        ping_times = []
        while len(ping_times) < args.size:
            time_begin = perf_counter()
            res = client.request("ping", wait_for_response=True)
            time_end = perf_counter()
            if res != "pong":
                log(ERROR, f"{batch_label}-> RES: unexpected response: {repr(res)}")
                raise StopIteration
            ping_times.append(time_end - time_begin)
        avg_time = avg(ping_times)
        total_ping_times.append(avg_time)
        log(COMM, f"{batch_label}-> ping {ms(avg_time)}")
        if args.interval > 0 and n_batch + 1 <= args.count:
            sleep(args.interval)
        n_batch += 1
except KeyboardInterrupt:
    pass
finally:
    if len(total_ping_times) > 1:
        log(INFO, f"Average ping: {ms(avg(total_ping_times))}")
    elif len(total_ping_times) == 1:
        log(INFO, f"Ping: {ms(avg(total_ping_times))}")
    log(DEBUG, f"Shutting down ...")
    client.shutdown()
    log(DEBUG, f"Shutdown complete. DONE!")
