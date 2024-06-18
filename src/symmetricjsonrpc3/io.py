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
"""IO wrappers for file-descriptor malleability."""
import errno
import fcntl
import io
import os
import socket
import sys
from logging import getLogger


logger = getLogger(__name__)


class Closable:
    """A context-manager that calls close() on exit.

    The close() does nothing by default; it's up to the
    inheritor to implement it.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """Do nothing by default."""
        pass


class Mode:
    """Get info from the "mode" text (as in `open(mode=...)`)."""

    def __init__(self, mode):
        self.mode = mode

    @property
    def read(self):
        """True if mode allows reading."""
        return any(flag in self.mode for flag in "r+")

    @property
    def write(self):
        """True if mode allows writing."""
        return any(flag in self.mode for flag in "aw+")

    @property
    def typemode(self):
        """Get 't' for text or 'b' for binary."""
        return 'b' if 'b' in self.mode else 't'

    @property
    def binary(self):
        """True if opened in binary mode."""
        return self.typemode == 'b'

    @property
    def text(self):
        """True if opened in text mode."""
        return self.typemode == 't'

    def __repr__(self):
        def b(v):
            return "1" if v else "0"
        return (f"Mode({self.mode},r={b(self.read)},w={b(self.write)},"
                f"tm={self.typemode},b={b(self.binary)},tx={b(self.text)})")


class _UnknownMode(Mode):
    read = True
    write = True
    typemode = ''

    def __init__(self):
        super().__init__("")


_m_unknown_mode = _UnknownMode()


def rwmode(fd):
    """Get read/write mode of a file-descriptor (or a file-like).

    The `fd` can be anything file-like that `makefile()` would also
    accept.

    Return "r", "w" or "r+". Return empty str if unable to
    determine. Return "w" also on "a" mode. Return "r+" on
    all "*+" modes.

    Raise OSError with errno.EBADF if it's not a file-descriptor
    nor a file-like.
    """
    def _fd_mode_methods_to_flags(fd, mode):
        """Heuristically discover file-like objects.

        Yeah, this is a guess-work, and an assumption that the methods
        that are named 'read' and 'write' actually behave as expected.
        """
        flags = ""
        if hasattr(fd, "read") and mode.read:
            flags += "r"
        if hasattr(fd, "write") and mode.write:
            flags += "w"
        if flags == "rw":
            flags = "r+"
        return flags

    # Step 1:
    # When we're dealing with a Python IOBase object, its mode
    # must be taken as the source of the absolute truth, regardless
    # the underlying's fileno flags.
    if isinstance(fd, io.IOBase) and hasattr(fd, "mode"):
        mode = Mode(fd.mode)
        return _fd_mode_methods_to_flags(fd, mode)

    # Step 2:
    # Try to get the info from the system first, if possible.
    try:
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        if (flags & os.O_WRONLY) == os.O_WRONLY:
            return "w"
        elif (flags & os.O_RDWR) == os.O_RDWR:
            return "r+"
        else:
            return "r"
    except (TypeError, io.UnsupportedOperation):
        pass

    # Step 3:
    # If the system fails, look around the object.
    if not hasattr(fd, "read") and not hasattr(fd, "write"):
        raise OSError(errno.EBADF, os.strerror(errno.EBADF), fd)

    flags = ""
    if hasattr(fd, "mode") and isinstance(fd.mode, str):
        mode = Mode(fd.mode)
    else:
        mode = _m_unknown_mode
    return _fd_mode_methods_to_flags(fd, mode)


def typemode(fd):
    """Get the text/binary mode of a file-descriptor (or a file-like).

    The `fd` can be anything file-like that `makefile()` would also
    accept.

    Return 't' for text, 'b' for binary or an empty str if unknown.

    Raise OSError with errno.EBADF if it's not a file-descriptor
    nor a file-like.
    """
    if isinstance(fd, int):
        fcntl.fcntl(fd, fcntl.F_GETFD)  # check if this is really an fd
        return "b"
    elif isinstance(fd, (io.RawIOBase, io.BufferedIOBase, socket.socket)):
        return "b"
    elif isinstance(fd, io.TextIOBase):
        return "t"
    elif hasattr(fd, "mode") and isinstance(fd.mode, str):
        return Mode(fd.mode).typemode
    return ""


class BytesIOWrapper(io.RawIOBase):
    def __init__(self, file, encoding=None, errors='strict'):
        self.file = file
        self.encoding = encoding or sys.getdefaultencoding()
        self.errors = errors
        self.buf = b''

    def readinto(self, buf):
        if not self.buf:
            self.buf = self.file.read(4096).encode(self.encoding, self.errors)
            if not self.buf:
                return 0
        length = min(len(buf), len(self.buf))
        buf[:length] = self.buf[:length]
        self.buf = self.buf[length:]
        return length

    def write(self, buf):
        text = buf.decode(self.encoding, self.errors)
        nwritten = self.file.write(text)
        return len(buf) if nwritten == len(text) else len(text[:nwritten].encode())


class ImmediateTextIOWrapper(io.TextIOWrapper):
    def __init__(self, buffer, encoding=None, errors=None, *args, **kwargs):
        kwargs["write_through"] = True
        super().__init__(buffer, encoding, errors, *args, **kwargs)

    def read(self, size=-1):
        self._checkReadable()
        chunk = self.buffer.read(size)
        return chunk.decode(self.encoding, self.errors)

    def __repr__(self):
        return (f"<symmetricjsonrpc3.io.ImmediateTextIOWrapper "
                f"encoding={self.encoding} buffer={self.buffer}>")


class SocketFile(io.RawIOBase):
    def __init__(self, sock, mode="r+b", encoding=None, errors="strict"):
        self._socket = sock
        self.mode = mode
        self._mode = Mode(mode)
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def write(self, data):
        if self._closed:
            raise ValueError("I/O operation on a closed socket")
        return self._socket.send(data)

    def flush(self):
        pass

    def readinto(self, buffer):
        if self._closed:
            raise ValueError("I/O operation on a closed socket")
        data = self._socket.recv(len(buffer))
        n = len(data)
        buffer[:n] = data
        return n

    def close(self):
        if not self._closed:
            self._socket.close()
            self._closed = True

    def fileno(self):
        return self._socket.fileno()

    def readable(self):
        return self._mode.read

    def writable(self):
        return self._mode.write

    def isatty(self):
        return False

    def getpeername(self):
        return self._socket.getpeername()

    @property
    def closed(self):
        return self._closed

    def __repr__(self):
        return f"<symmetricjsonrpc3.io.SocketFile{self._socket}>"


def makefile(fd, mode=None, **kwargs):
    """Wrap anything file-like in a file-like object with a common interface.

    The `fd` is assumed to be an open file (i.e. not a path).

    If `fd`:

    - is an int -- consider it a file-descriptor and os.fdopen() it,
    - looks like a socket -- socket.makefile() it, with a monkey-patched
      close() that will actually call socket.close(),
    - looks like a file-like object already,
      - and matches the text/binary mode -- just return it,
      - and has a different text/binary mode -- wrap it in a codec
        (and monkey-patch close()),
    - none of the above -- raise TypeError.

    The `mode` is as in `open()`, but it will be tested against the `fd`
    to check if the read-write mode matches, and if conversion between
    binary or text is needed. Truncation, repositioning and appending
    modes may be ignored by the wrapper. If `None`, `makefile` will try
    to match the `fd` mode.

    If the requested read-write `mode` doesn't match the mode of `fd`,
    ValueError is raised.

    If the text/binary `mode` differs, `fd` will be put into a
    conversion wrapper. The `encoding`, `errors`, et al parameters can
    be embedded in **kwargs.

    Calling close() on the returned wrapper will also close the `fd`.

    """
    wrapper = None
    if isinstance(fd, int):
        if mode is None:
            mode = rwmode(fd) + "b"
        return os.fdopen(fd, mode=mode, **kwargs)
    elif isinstance(fd, io.IOBase):
        fd_rwmode = rwmode(fd)
        fd_typemode = typemode(fd)
        fd_mode = Mode(fd_rwmode + fd_typemode)
        req_mode = Mode(mode) if mode is not None else fd_mode
        if ((req_mode.write and not fd_mode.write)
                or (req_mode.read and not fd_mode.read)):
            raise ValueError(f"read-write mode mismatch mode={mode},fd.mode={fd_mode.mode}")
        if req_mode.typemode == fd_mode.typemode:
            return fd
        else:
            # Wrap into a converter.
            if req_mode.text:
                # binary fd to text wrapper
                wrapper = io.TextIOWrapper(fd, **kwargs)
            else:
                # text fd to binary wrapper
                wrapper = BytesIOWrapper(fd, **kwargs)

            # Monkey-patch the wrapper's close function so that
            # closing the wrapper also closes the underlying file.
            original_close = wrapper.close

            def monkey_close(*args, **kwargs):
                original_close(*args, **kwargs)
                fd.close()
            wrapper.close = monkey_close
    elif isinstance(fd, socket.socket):
        if mode is None:
            mode = "r+b"
        req_mode = Mode(mode)
        wrapper = SocketFile(fd, mode, **kwargs)
        if req_mode.text:
            wrapper = ImmediateTextIOWrapper(wrapper, **kwargs)
    else:
        raise TypeError(f"don't know how to make a file out of {type(fd)}")

    return wrapper
