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

    def __new__(cls, f):
        if cls is not WriterWrapper:
            return object.__new__(cls)
        elif hasattr(f, "send"):
            return SocketWriter(f)
        elif hasattr(f, "write"):
            return FileWriter(f)
        else:
            return f

    def __init__(self, f):
        self.f = f
        self.buff = None
        self.poll = None
        if _is_real_file(f):
            self.poll = select.poll()
            self.poll.register(f, select.POLLOUT | select.POLLERR | select.POLLHUP | select.POLLNVAL)
        self.closed = False

    @property
    def buff_len(self):
        return len(self.buff) if self.buff else 0

    def close(self):
        self.closed = True
        self.f.close()

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
        self.f.write(s)
        return len(s)


class SocketWriter(WriterWrapper):
    def _write(self, s):
        res = self.f.send(s.encode('ascii'))
        return res


class ReaderWrapper:
    """Provides a unified interface for reading from sockets or
    file-like objects.

    Its instances will actually belong to one of its subclasses,
    depending on what type of object it wraps."""
    poll_timeout = 1000

    def __new__(cls, f):
        if cls is not ReaderWrapper:
            return object.__new__(cls)
        elif hasattr(f, "recv"):
            return SocketReader(f)
        elif hasattr(f, "read"):
            return FileReader(f)
        else:
            return f

    def __init__(self, f):
        self.file = f
        self.poll = None
        if _is_real_file(f):
            self.poll = select.poll()
            self.poll.register(f, select.POLLIN | select.POLLPRI | select.POLLERR | select.POLLHUP | select.POLLNVAL)
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

    def close(self):
        self.closed = True
        self.file.close()

    def _wait(self):
        if not self.poll:
            return
        res = []
        while not res and not self.closed:
            res = self.poll.poll(self.poll_timeout)
        if self.closed:
            raise EOFError

    def _read(self):
        raise NotImplementedError


class FileReader(ReaderWrapper):
    def _read(self):
        return self.file.read(1)


class SocketReader(ReaderWrapper):
    def _read(self):
        return self.file.recv(1).decode('ascii')


class ReIterator:
    """An iterator wrapper that provides lookahead through the peek
    method."""
    def __init__(self, i):
        self._prefix = [] # In reverse order!
        self._i = iter(i)

    def __iter__(self):
        return self

    def next(self):
        if self._prefix:
            return self._prefix.pop(0)
        return next(self._i)

    def _put(self, value):
        self._prefix.append(value)

    def peek(self):
        try:
            if not self._prefix:
                self._put(next(self._i))
            return self._prefix[-1]
        except StopIteration:
            raise EOFError()


def _is_real_file(f):
    if hasattr(f, 'fileno'):
        try:
            return f.fileno() is not None
        except io.UnsupportedOperation:
            pass
    return False
