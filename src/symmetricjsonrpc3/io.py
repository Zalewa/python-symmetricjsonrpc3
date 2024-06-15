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
import queue
import selectors
import socket
import sys
import threading
from logging import getLogger


logger = getLogger(__name__)


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
    elif isinstance(fd, socket.socket):
        req_mode = Mode(mode or "rwb")
        sockmode = (("r" if req_mode.read else "")
                    + ("w" if req_mode.write else "")
                    + ("b" if req_mode.binary else ""))
        wrapper = fd.makefile(mode=sockmode, **kwargs)
        try:
            wrapper.fileno()
        except io.UnsupportedOperation:
            # monkey-patch BufferedRWPair that yells UnsupportedOperation here
            wrapper.fileno = lambda: fd.fileno()
    else:
        raise TypeError(f"don't know how to make a file out of {type(fd)}")

    # Monkey-patch the wrapper's close function so that
    # closing the wrapper also closes the underlying file.
    original_close = wrapper.close

    def monkey_close(*args, **kwargs):
        original_close(*args, **kwargs)
        fd.close()
    wrapper.close = monkey_close
    return wrapper


class _Reselector:
    def __init__(self, selector, parent):
        self.selector = selector
        self.parent = parent
        self._oldflags = 0

    @property
    def events(self):
        return self._oldflags

    @property
    def fd(self):
        return self.parent.fd

    def modify(self, flags):
        if flags == self._oldflags:
            self._log_debug("I")
            return

        if self._oldflags == 0:
            self._log_debug("R: %s", flags)
            self.selector.register(self.fd, flags)
        elif flags == 0:
            self._log_debug("U")
            self.selector.unregister(self.fd)
        else:
            self._log_debug("M: %s -> %s", self._oldflags, flags)
            self.selector.modify(self.fd, flags)
        self._oldflags = flags

    def _log_debug(self, fmt, *args, **kwargs):
        self.parent._log_debug("Reselector: " + fmt, *args, **kwargs)


class _IoJob:
    name = "io"

    def __init__(self):
        self.source = threading.current_thread().name
        self.result = None
        self.error = None
        self.complete = threading.Event()

    def get_result(self):
        self.complete.wait()
        if self.error:
            raise self.error
        return self.result

    def accept(self, result):
        self.result = result
        self.complete.set()

    def reject(self, error):
        self.error = error
        self.complete.set()


class _ImmediateJob(_IoJob):
    pass


class _FlushJob(_ImmediateJob):
    name = "flush"

    def run(self, fd):
        self.accept(fd.flush())


class _WriteJob(_IoJob):
    name = "write"

    def __init__(self, data):
        super().__init__()
        self.data = data

    def run(self, fd):
        self.accept(fd.write(self.data))


class _WriteAtomicJob(_WriteJob):
    name = "write-atomic"

    def __init__(self, data):
        super().__init__(data)
        self._nwritten = 0

    def run(self, fd):
        data = self.data
        if self._nwritten > 0:
            data = data[self._nwritten:]
        self._nwritten += fd.write(data)
        if len(self.data) == self._nwritten:
            self.accept(self._nwritten)


class _ReadJob(_IoJob):
    name = "read"

    def __init__(self, n):
        super().__init__()
        self.amount = n

    def run(self, fd):
        self.accept(fd.read(self.amount))


class _IoJobQueue:
    def __init__(self):
        self.read_jobs = []
        self.write_jobs = []

    @property
    def all_jobs(self):
        return self.read_jobs + self.write_jobs

    def pop_all(self):
        jobs = self.all_jobs
        self.clear()
        return jobs

    def clear(self):
        self.read_jobs = []
        self.write_jobs = []

    def enqueue(self, job):
        if isinstance(job, _ReadJob):
            self.read_jobs.append(job)
        elif isinstance(job, _WriteJob):
            self.write_jobs.append(job)
        else:
            raise TypeError(f"unknown job type '{type(job)}'")

    def enqueue_all(self, jobqueue):
        while True:
            try:
                self.enqueue(jobqueue.get(block=False))
            except TypeError:
                continue
            except queue.Empty:
                break

    def accept_empty_all(self):
        for job in self.read_jobs:
            job.accept(b'')
        for job in self.write_jobs:
            job.accept(0)
        self.clear()

    def reject_all(self, e):
        for job in self.pop_all():
            job.reject(e)

    def __len__(self):
        return len(self.read_jobs) + len(self.write_jobs)


class SyncIO(threading.Thread):
    def __init__(self, fd, mode="r+b", log=False):
        if log:
            logger.debug("SyncIO(fd=%s,mode=%s)", fd, mode)
        super().__init__(name=f"SyncIO-{fd}")
        self.daemon = True
        self.fd = makefile(fd, mode=mode)
        self.log = log
        try:
            os.set_blocking(self.fd.fileno(), False)
        except io.UnsupportedOperation:
            # Not a real file, so I guess it's nonblocking by default (?).
            # We won't be able to select on it either.
            pass
        self._modeflags = 0

        fdmode = Mode(rwmode(fd))
        if fdmode.read:
            self._modeflags |= selectors.EVENT_READ
        if fdmode.write:
            self._modeflags |= selectors.EVENT_WRITE
        if self._modeflags == 0:
            raise ValueError("must have a read, write or read/write mode")

        self._queue = queue.Queue()
        self._rjob, self._wjob = os.pipe()

        self._closed = False
        self._close_lock = threading.Lock()

        self._selector = selectors.DefaultSelector()
        self._selector.register(self._rjob, selectors.EVENT_READ)

        # We will be selecting on the fd, but only if it can be selected on.
        # If not, we'll assume it's always ready for IO.
        try:
            self._selector.register(self.fd, selectors.EVENT_READ
                                    | selectors.EVENT_WRITE)
        except ValueError:
            self._reselector = None
        else:
            # Let the Reselector take care of this fd from now on.
            self._selector.unregister(self.fd)
            self._reselector = _Reselector(self._selector, self)

        self.start()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def write(self, data):
        if not (self._modeflags & selectors.EVENT_WRITE):
            raise io.UnsupportedOperation('not writable')
        return self._run_job(_WriteAtomicJob(data))

    def read(self, n=None):
        if not (self._modeflags & selectors.EVENT_READ):
            raise io.UnsupportedOperation('not readable')
        return self._run_job(_ReadJob(n))

    def flush(self):
        return self._run_job(_FlushJob())

    def _run_job(self, job):
        with self._close_lock:
            if not self._closed:
                self._queue.put(job)
                os.write(self._wjob, b'j')
            else:
                job.reject(ValueError("I/O operation on a closed stream"))
        return job.get_result()

    def close(self):
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            os.write(self._wjob, b'q')
        self.join()

    def run(self):
        iojobs = _IoJobQueue()
        try:
            while not self._closed:
                self._log_debug(
                    "-> selecting (%s) ... <-",
                    f"{self.fd.fileno()},{self._reselector.events}" if self._reselector else "RAM")
                events = self._selector.select()
                rlist = [event.fd for event, mask in events
                         if mask & selectors.EVENT_READ]
                wlist = [event.fd for event, mask in events
                         if mask & selectors.EVENT_WRITE]
                self._log_debug("select r=%s w=%s; jobs r=%s w=%s",
                                rlist, wlist,
                                len(iojobs.read_jobs),
                                len(iojobs.write_jobs))

                if self._rjob in rlist:
                    signal = os.read(self._rjob, 1)
                    if signal == b'q':
                        self._closed = True
                    elif signal == b'j':
                        job = self._queue.get()
                        if isinstance(job, _ImmediateJob):
                            self._log_debug("immediate %s job from %s",
                                            job.name, job.source)
                            job.run(self.fd)
                        else:
                            self._log_debug("enqueueing %s job from %s",
                                            job.name, job.source)
                            iojobs.enqueue(job)
                    else:
                        raise TypeError(f"unknown job signal '{signal}'")

                if self._reselector:
                    fd_ready = ((selectors.EVENT_WRITE if self.fd.fileno() in wlist else 0)
                                | (selectors.EVENT_READ if self.fd.fileno() in rlist else 0))
                    selectorflags = self._modeflags & \
                        ((selectors.EVENT_WRITE if iojobs.write_jobs else 0)
                         | (selectors.EVENT_READ if iojobs.read_jobs else 0))
                    self._reselector.modify(selectorflags)
                else:
                    fd_ready = selectors.EVENT_WRITE | selectors.EVENT_READ

                joblist = None
                if iojobs.read_jobs and (fd_ready & selectors.EVENT_READ):
                    joblist = iojobs.read_jobs
                elif iojobs.write_jobs and (fd_ready & selectors.EVENT_WRITE):
                    joblist = iojobs.write_jobs
                if joblist:
                    job = joblist[0]
                    self._log_debug("running %s job from %s", job.name, job.source)
                    try:
                        job.run(self.fd)
                    except OSError as e:
                        job.reject(e)
                        raise
                    if job.complete.is_set():
                        del joblist[0]
            with self._close_lock:
                self._closed = True
                iojobs.enqueue_all(self._queue)
            iojobs.accept_empty_all()
        except BaseException as e:
            with self._close_lock:
                self._closed = True
                iojobs.enqueue_all(self._queue)
            iojobs.reject_all(e)
            raise
        finally:
            os.close(self._wjob)
            os.close(self._rjob)
            self.fd.close()

    def _log_debug(self, fmt, *args, **kwargs):
        if self.log:
            logger.debug("SyncIO(%s): " + fmt, self.fd, *args, **kwargs)
