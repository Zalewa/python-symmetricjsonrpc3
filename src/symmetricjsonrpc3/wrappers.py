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

"""Utilities for abstracting I/O for sockets or file-like objects
behind an identical interface."""
import io
import select
from logging import getLogger


logger = getLogger(__name__)

debug_write = False
debug_read = False


class WriterWrapper:
    """Provides a unified interface for writing to sockets or
    file-like objects.

    Its instances will actually belong to one of its subclasses,
    depending on what type of object it wraps."""
    poll_timeout = 1000
    buff_maxsize = 512

    def __new__(cls, stream):
        if cls is not WriterWrapper:
            return object.__new__(cls)
        elif hasattr(stream, "send"):
            return SocketWriter(stream)
        elif hasattr(stream, "write"):
            return FileWriter(stream)
        else:
            return stream

    def __init__(self, stream):
        self.stream = stream
        self.buff = None
        self.poll = None
        if _is_real_file(stream):
            self.poll = select.poll()
            self.poll.register(stream, select.POLLOUT | select.POLLERR | select.POLLHUP | select.POLLNVAL)
        self.closed = False

    @property
    def buff_len(self):
        return len(self.buff) if self.buff else 0

    def close(self):
        self.closed = True
        self.stream.close()

    def write(self, s):
        if self.buff is None:
            self.buff = s
        else:
            self.buff += s
        if self.buff_len > self.buff_maxsize:
            self.flush()

    def flush(self):
        data = self.buff
        self.buff = None
        if debug_write:
            logger.debug("write(%s)", repr(data))
        while data:
            self._wait()
            data = data[self._write(data):]

    def _wait(self):
        if not self.poll:
            return
        res = []
        while not res and not self.closed:
            res = self.poll.poll(self.poll_timeout)
        if self.closed:
            raise EOFError

    def _write(self, s):
        raise NotImplementedError


class FileWriter(WriterWrapper):
    def _write(self, s):
        self.stream.write(s)
        return len(s)


class SocketWriter(WriterWrapper):
    def _write(self, s):
        res = self.stream.send(s.encode('ascii'))
        return res


class ReaderWrapper:
    """Provides a unified interface for reading from sockets,
    file-like objects or even strings.

    Its instances will actually belong to one of its subclasses,
    depending on what type of object it wraps."""
    poll_timeout = 1000

    def __new__(cls, stream):
        def get_cls():
            if cls is not ReaderWrapper:
                return cls
            elif isinstance(stream, bytes):
                return BytesReader
            elif isinstance(stream, str):
                return StringReader
            elif hasattr(stream, "recv"):
                return SocketReader
            elif hasattr(stream, "read"):
                return FileReader
            return None
        cls = get_cls()
        return super().__new__(cls) if cls is not None else stream

    def __init__(self, stream):
        self.stream = stream
        self.poll = None
        if _is_real_file(stream):
            self.poll = select.poll()
            self.poll.register(stream, select.POLLIN | select.POLLPRI | select.POLLERR | select.POLLHUP | select.POLLNVAL)
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        try:
            self._wait()
        except EOFError:
            raise StopIteration
        result = self._read()
        if result == '':
            raise StopIteration
        else:
            if debug_read:
                logger.debug("read(%s)", repr(result))
            return result

    def read(self, n=None):
        try:
            self._wait()
        except EOFError:
            chunk = ''
        else:
            chunk = self._read(n)
            if debug_read:
                logger.debug("read(%s)", repr(chunk))
        return chunk

    def close(self):
        self.closed = True
        self.stream.close()

    def _wait(self):
        if not self.poll:
            return
        res = []
        while not res and not self.closed:
            res = self.poll.poll(self.poll_timeout)
        if self.closed:
            raise EOFError

    def _read(self, n):
        raise NotImplementedError


class FileReader(ReaderWrapper):
    def _read(self, n):
        return self.stream.read(n)


class BytesReader(FileReader):
    def __init__(self, f):
        super().__init__(io.BytesIO(f))


class StringReader(FileReader):
    def __init__(self, f):
        super().__init__(io.StringIO(f))


class SocketReader(ReaderWrapper):
    def _read(self, n):
        if n is None or n <= 0:
            n = 1024
        chunk = self.stream.recv(n)
        return chunk.decode('ascii') if chunk else ''


def _is_real_file(f):
    if hasattr(f, 'fileno'):
        try:
            return f.fileno() is not None
        except io.UnsupportedOperation:
            pass
    return False
