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
import io
import os
import queue
import selectors
import threading
from logging import getLogger


logger = getLogger(__name__)


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


class _WriteJob(_IoJob):
    name = "write"

    def __init__(self, data):
        super().__init__()
        self.data = data

    def run(self, fd):
        self.accept(os.write(fd, self.data))


class _WriteAtomicJob(_WriteJob):
    name = "write-atomic"

    def __init__(self, data):
        super().__init__(data)
        self._nwritten = 0

    def run(self, fd):
        data = self.data
        if self._nwritten > 0:
            data = data[self._nwritten:]
        self._nwritten += os.write(fd, data)
        if len(self.data) == self._nwritten:
            self.accept(self._nwritten)


class _ReadJob(_IoJob):
    name = "read"

    def __init__(self, n):
        super().__init__()
        self.amount = n

    def run(self, fd):
        self.accept(os.read(fd, self.amount))


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
    def __init__(self, fd, mode="rw", log=False):
        if log:
            logger.debug("SyncIO(fd=%s,mode=%s)", fd, mode)
        super().__init__(name=f"SyncIO-{fd}")
        self.daemon = True
        self.fd = fd
        self.log = log
        os.set_blocking(fd, False)
        self._modeflags = 0

        for c in mode:
            # 'b' is ignored
            if c not in "rwb":
                raise ValueError(f"unknown mode '{c}'")
        if 'r' in mode:
            self._modeflags |= selectors.EVENT_READ
        if 'w' in mode:
            self._modeflags |= selectors.EVENT_WRITE
        if self._modeflags == 0:
            raise ValueError("must have a read, write or read/write mode")

        self._queue = queue.Queue()
        self._rjob, self._wjob = os.pipe()

        self._closed = False
        self._close_lock = threading.Lock()

        self._selector = selectors.DefaultSelector()
        self._selector.register(self._rjob, selectors.EVENT_READ)
        self._reselector = _Reselector(self._selector, self)

        self.start()

    def write(self, data):
        if not (self._modeflags & selectors.EVENT_WRITE):
            raise io.UnsupportedOperation('not writable')
        return self._run_job(_WriteAtomicJob(data))

    def read(self, n):
        if not (self._modeflags & selectors.EVENT_READ):
            raise io.UnsupportedOperation('not readable')
        return self._run_job(_ReadJob(n))

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
                self._log_debug("-> selecting (%s) ... <-", self._reselector.events)
                events = self._selector.select()
                rlist = [event.fd for event, mask in events
                         if mask & selectors.EVENT_READ]
                wlist = [event.fd for event, mask in events
                         if mask & selectors.EVENT_WRITE]
                self._log_debug("spin r=%s w=%s", rlist, wlist)

                if self._rjob in rlist:
                    signal = os.read(self._rjob, 1)
                    if signal == b'q':
                        self._closed = True
                    elif signal == b'j':
                        job = self._queue.get()
                        self._log_debug("enqueueing %s job from %s",
                                        job.name, job.source)
                        iojobs.enqueue(job)
                    else:
                        raise TypeError(f"unknown job signal '{signal}'")

                selectorflags = (self._modeflags &
                                 ((selectors.EVENT_WRITE if iojobs.write_jobs else 0) |
                                  (selectors.EVENT_READ if iojobs.read_jobs else 0)
                                  )
                                 )

                self._reselector.modify(selectorflags)

                joblist = None
                if iojobs.read_jobs and self.fd in rlist:
                    joblist = iojobs.read_jobs
                elif iojobs.write_jobs and self.fd in wlist:
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
            os.close(self.fd)

    def _log_debug(self, fmt, *args, **kwargs):
        if self.log:
            logger.debug("SyncIO(%s): " + fmt, self.fd, *args, **kwargs)
