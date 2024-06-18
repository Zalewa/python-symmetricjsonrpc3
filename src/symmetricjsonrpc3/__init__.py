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
"""A symmetric and transport agnostic JSON-RPC 2.0 implementation."""

__all__ = [
    "ClientConnection",
    "Connection",
    "RPCClient",
    "RPCError",
    "RPCServer",
    "Reader",
    "ServerConnection",
    "Thread",
    "ThreadedClient",
    "Writer",
    "from_json",
    "to_json"
]

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("symmetricjsonrpc3")
except PackageNotFoundError:
    __version__ = "unknown"

from .dispatcher import Connection, ServerConnection, Thread, ThreadedClient
from .json import Reader, Writer, from_json, to_json
from .rpc import ClientConnection, RPCClient, RPCError, RPCServer
