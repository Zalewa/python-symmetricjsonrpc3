#!/usr/bin/env pytest

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
import errno
import fcntl
import io
import os
import socket
import tempfile
import unittest

from symmetricjsonrpc3.io import makefile, rwmode, typemode, Mode, SyncIO


class TestMode(unittest.TestCase):
    def test_read(self):
        self.assertTrue(Mode("r").read)
        self.assertTrue(Mode("+").read)
        self.assertTrue(Mode("w+").read)
        self.assertTrue(Mode("rb").read)
        self.assertTrue(Mode("+b").read)
        self.assertTrue(Mode("w+b").read)
        self.assertFalse(Mode("w").read)
        self.assertFalse(Mode("a").read)
        self.assertFalse(Mode("wb").read)
        self.assertFalse(Mode("ab").read)

    def test_write(self):
        self.assertTrue(Mode("w").write)
        self.assertTrue(Mode("+").write)
        self.assertTrue(Mode("w+").write)
        self.assertTrue(Mode("a").write)
        self.assertTrue(Mode("wb").write)
        self.assertTrue(Mode("+b").write)
        self.assertTrue(Mode("w+b").write)
        self.assertTrue(Mode("ab").write)
        self.assertFalse(Mode("r").write)
        self.assertFalse(Mode("rb").write)

    def test_typemode(self):
        self.assertEqual(Mode("r").typemode, 't')
        self.assertEqual(Mode("w").typemode, 't')
        self.assertEqual(Mode("+").typemode, 't')
        self.assertEqual(Mode("w+").typemode, 't')
        self.assertEqual(Mode("a").typemode, 't')

        self.assertEqual(Mode("rt").typemode, 't')
        self.assertEqual(Mode("wt").typemode, 't')
        self.assertEqual(Mode("+t").typemode, 't')
        self.assertEqual(Mode("w+t").typemode, 't')
        self.assertEqual(Mode("at").typemode, 't')

        self.assertEqual(Mode("rb").typemode, 'b')
        self.assertEqual(Mode("wb").typemode, 'b')
        self.assertEqual(Mode("+b").typemode, 'b')
        self.assertEqual(Mode("w+b").typemode, 'b')
        self.assertEqual(Mode("ab").typemode, 'b')

    def test_binary(self):
        self.assertFalse(Mode("r").binary)
        self.assertFalse(Mode("w").binary)
        self.assertFalse(Mode("+").binary)
        self.assertFalse(Mode("w+").binary)
        self.assertFalse(Mode("a").binary)

        self.assertFalse(Mode("rt").binary)
        self.assertFalse(Mode("wt").binary)
        self.assertFalse(Mode("+t").binary)
        self.assertFalse(Mode("w+t").binary)
        self.assertFalse(Mode("at").binary)

        self.assertTrue(Mode("rb").binary)
        self.assertTrue(Mode("wb").binary)
        self.assertTrue(Mode("+b").binary)
        self.assertTrue(Mode("w+b").binary)
        self.assertTrue(Mode("ab").binary)

    def test_text(self):
        self.assertTrue(Mode("r").text)
        self.assertTrue(Mode("w").text)
        self.assertTrue(Mode("+").text)
        self.assertTrue(Mode("w+").text)
        self.assertTrue(Mode("a").text)

        self.assertTrue(Mode("rt").text)
        self.assertTrue(Mode("wt").text)
        self.assertTrue(Mode("+t").text)
        self.assertTrue(Mode("w+t").text)
        self.assertTrue(Mode("at").text)

        self.assertFalse(Mode("rb").text)
        self.assertFalse(Mode("wb").text)
        self.assertFalse(Mode("+b").text)
        self.assertFalse(Mode("w+b").text)
        self.assertFalse(Mode("ab").text)


class TestRwmode(unittest.TestCase):
    def test_pipe(self):
        r, w = os.pipe()
        try:
            self.assertEqual(rwmode(r), "r")
            self.assertEqual(rwmode(w), "w")
        finally:
            os.close(r)
            os.close(w)

    def test_meaningless_int(self):
        # Find an fd that doesn't exist.
        for fd in range(300000, 300000 * 10):
            try:
                fcntl.fcntl(fd, fcntl.F_GETFD)
            except OSError as error:
                if error.errno == errno.EBADF:
                    break  # got it!
        else:
            raise Exception("test error -- couldn't find a free file-descriptor")

        with self.assertRaises(OSError) as error:
            rwmode(fd)
        self.assertEqual(error.exception.errno, errno.EBADF)

    def test_socket(self):
        with socket.socket() as sock:
            self.assertEqual(rwmode(sock), "r+")

    def test_stringio(self):
        with io.StringIO() as strio:
            self.assertEqual(rwmode(strio), "r+")

    def test_bytesio(self):
        with io.BytesIO() as bytesio:
            self.assertEqual(rwmode(bytesio), "r+")

    def test_tempfile(self):
        with tempfile.TemporaryFile() as temp:
            self.assertEqual(rwmode(temp), "r+")
        with tempfile.TemporaryFile(mode="r") as read_only_temp:
            self.assertEqual(rwmode(read_only_temp), "r")
        with tempfile.TemporaryFile(mode="w") as write_only_temp:
            self.assertEqual(rwmode(write_only_temp), "w")

    def test_fakeio(self):
        class _Moder:
            def __init__(self, mode=None):
                if mode is not None:
                    self.mode = mode

        class _Reader(_Moder):
            def read(self, *args, **kwargs): pass

        class _Writer(_Moder):
            def write(self, *args, **kwargs): pass

        class _RW(_Reader, _Writer):
            pass

        self.assertEqual(rwmode(_Reader()), "r")
        self.assertEqual(rwmode(_Reader("r")), "r")
        self.assertEqual(rwmode(_Reader("w")), "")
        self.assertEqual(rwmode(_Reader("+")), "r")
        self.assertEqual(rwmode(_Reader("a")), "")
        self.assertEqual(rwmode(_Writer()), "w")
        self.assertEqual(rwmode(_Writer("r")), "")
        self.assertEqual(rwmode(_Writer("r+")), "w")
        self.assertEqual(rwmode(_Writer("+")), "w")
        self.assertEqual(rwmode(_Writer("w")), "w")
        self.assertEqual(rwmode(_Writer("a")), "w")
        self.assertEqual(rwmode(_RW()), "r+")
        self.assertEqual(rwmode(_RW("r")), "r")
        self.assertEqual(rwmode(_RW("w")), "w")
        self.assertEqual(rwmode(_RW("a")), "w")
        self.assertEqual(rwmode(_RW("r+")), "r+")
        self.assertEqual(rwmode(_RW("w+")), "r+")
        self.assertEqual(rwmode(_RW("a+")), "r+")

    def test_not_filelike(self):
        with self.assertRaises(OSError) as error:
            rwmode(object)
        self.assertEqual(error.exception.errno, errno.EBADF)


class TestTypemode(unittest.TestCase):
    def test_pipe(self):
        r, w = os.pipe()
        try:
            self.assertEqual(typemode(r), "b")
            self.assertEqual(typemode(w), "b")
        finally:
            os.close(r)
            os.close(w)

    def test_meaningless_int(self):
        # Find an fd that doesn't exist.
        for fd in range(300000, 300000 * 10):
            try:
                fcntl.fcntl(fd, fcntl.F_GETFD)
            except OSError as error:
                if error.errno == errno.EBADF:
                    break  # got it!
        else:
            raise Exception("test error -- couldn't find a free file-descriptor")

        with self.assertRaises(OSError) as error:
            typemode(fd)
        self.assertEqual(error.exception.errno, errno.EBADF)

    def test_socket(self):
        with socket.socket() as sock:
            self.assertEqual(typemode(sock), "b")

    def test_stringio(self):
        with io.StringIO() as strio:
            self.assertEqual(typemode(strio), "t")

    def test_bytesio(self):
        with io.BytesIO() as bytesio:
            self.assertEqual(typemode(bytesio), "b")

    def test_tempfile(self):
        with tempfile.TemporaryFile() as temp_file:
            self.assertEqual(typemode(temp_file), "b")
        with tempfile.TemporaryFile("w+") as text_temp_file:
            self.assertEqual(typemode(text_temp_file), "t")
        with tempfile.TemporaryFile(mode="r+b") as binary_temp_file:
            self.assertEqual(typemode(binary_temp_file), "b")

    def test_mode(self):
        class _Moder:
            def __init__(self, mode):
                self.mode = mode

        self.assertEqual(typemode(_Moder("")), "t")
        self.assertEqual(typemode(_Moder("r")), "t")
        self.assertEqual(typemode(_Moder("w")), "t")
        self.assertEqual(typemode(_Moder("r+")), "t")
        self.assertEqual(typemode(_Moder("a")), "t")
        self.assertEqual(typemode(_Moder("r+")), "t")
        self.assertEqual(typemode(_Moder("t")), "t")
        self.assertEqual(typemode(_Moder("rt")), "t")
        self.assertEqual(typemode(_Moder("wt")), "t")
        self.assertEqual(typemode(_Moder("r+t")), "t")
        self.assertEqual(typemode(_Moder("at")), "t")
        self.assertEqual(typemode(_Moder("r+t")), "t")
        self.assertEqual(typemode(_Moder("b")), "b")
        self.assertEqual(typemode(_Moder("rb")), "b")
        self.assertEqual(typemode(_Moder("wb")), "b")
        self.assertEqual(typemode(_Moder("r+b")), "b")
        self.assertEqual(typemode(_Moder("ab")), "b")
        self.assertEqual(typemode(_Moder("r+b")), "b")
        self.assertEqual(typemode(object), "")


class TestMakefile(unittest.TestCase):
    """Test a variety of fake and real files."""
    def test_stringio(self):
        strio = io.StringIO()
        try:
            wrapper = makefile(strio)
        except TypeError as error:
            self.fail(f"unexpected TypeError: {error}")
        try:
            self.assertIs(wrapper, strio)
        finally:
            wrapper.close()
        self.assertTrue(strio.closed)

    def test_bytesio(self):
        bytesio = io.BytesIO()
        try:
            wrapper = makefile(bytesio)
        except TypeError as error:
            self.fail(f"unexpected TypeError: {error}")
        try:
            self.assertIs(wrapper, bytesio)
        finally:
            wrapper.close()
        self.assertTrue(bytesio.closed)

    def test_pipe(self):
        pipe_r, pipe_w = os.pipe()
        os.close(pipe_w)
        try:
            wrapper = makefile(pipe_r)
        except TypeError as error:
            os.close(pipe_r)
            self.fail(f"unexpected TypeError: {error}")
        try:
            self.assertIsNot(pipe_r, wrapper)
            self.assertIsInstance(wrapper, io.IOBase)
            self.assertEqual(pipe_r, wrapper.fileno())
        finally:
            wrapper.close()
        # check if pipe_r is closed
        with self.assertRaises(OSError) as error:
            fcntl.fcntl(pipe_r, fcntl.F_GETFL)
        self.assertEqual(error.exception.errno, errno.EBADF)

    def test_socket(self):
        sock = socket.socket()
        try:
            wrapper = makefile(sock)
        except TypeError as error:
            sock.close()
            self.fail(f"unexpected TypeError: {error}")
        try:
            self.assertIsNot(wrapper, sock)
            self.assertIsInstance(wrapper, io.IOBase)
            self.assertEqual(wrapper.fileno(), sock.fileno())
        finally:
            wrapper.close()
        # socket's fileno() returns -1 when the socket is closed
        self.assertEqual(sock.fileno(), -1)

    def test_socket_default_mode(self):
        with makefile(socket.socket()) as file:
            self.assertIsInstance(file, (io.RawIOBase, io.BufferedIOBase))

    def test_socket_binary_mode(self):
        with makefile(socket.socket(), "r+b") as file:
            self.assertIsInstance(file, (io.RawIOBase, io.BufferedIOBase))

    def test_socket_text_mode(self):
        with makefile(socket.socket(), "r+t") as file:
            self.assertIsInstance(file, io.TextIOBase)

    def test_socket_modes(self):
        modes = ["r+", "r+b", "w+", "w+b", "ab", "a+b"]
        sock = socket.socket()
        try:
            for mode in modes:
                makefile(sock, mode=mode)
        except ValueError as error:
            self.fail(f"failed on mode '{mode}': {error}")
        finally:
            sock.close()

    def test_socketpair_io_binary_read(self):
        s1, s2 = socket.socketpair()
        try:
            with makefile(s2, "rb") as sf2:
                s1.send(b'watermelon\n')
                self.assertEqual(sf2.read(1024), b'watermelon\n')
        finally:
            s1.close()

    def test_socketpair_io_binary_write(self):
        s1, s2 = socket.socketpair()
        try:
            with makefile(s2, "wb") as sf2:
                sf2.write(b'pineapple\n')
                sf2.flush()
                self.assertEqual(s1.recv(1024), b'pineapple\n')
        finally:
            s1.close()

    def test_tempfile(self):
        file = tempfile.TemporaryFile()
        try:
            wrapper = makefile(file)
        except TypeError as error:
            self.fail(f"unexpected TypeError: {error}")
            file.close()
        try:
            self.assertIs(file, wrapper)
        finally:
            wrapper.close()
        self.assertTrue(file.closed)

    def test_tempfile_different_rw_mode(self):
        with tempfile.TemporaryFile(mode="w") as write_file:
            with self.assertRaises(ValueError):
                makefile(write_file, mode="r")
        with tempfile.TemporaryFile(mode="r") as read_file:
            with self.assertRaises(ValueError):
                makefile(read_file, mode="w")

    def test_text_file_in_binary_mode(self):
        text_file = tempfile.TemporaryFile(mode="w+")
        text_file.write("watermel\u00F6n\n")
        text_file.flush()
        text_file.seek(0)

        with makefile(text_file, mode="w+b", encoding='utf-8') as binary_wrapper:
            self.assertEqual(binary_wrapper.read(), b"watermel\xC3\xB6n\n")
        self.assertTrue(text_file.closed)

    def test_binary_file_in_text_mode(self):
        binary_file = tempfile.TemporaryFile(mode="w+b")
        binary_file.write(b"watermel\xC3\xB6n\n")
        binary_file.flush()
        binary_file.seek(0)

        with makefile(binary_file, mode="w+", encoding='utf-8') as text_wrapper:
            self.assertEqual(text_wrapper.read(), "watermel\u00F6n\n")
        self.assertTrue(binary_file.closed)

    def test_not_makeable(self):
        with self.assertRaises(TypeError):
            makefile("/tmp/watermelon")


class TestSyncIO(unittest.TestCase):
    # ** test_openclose_... **
    # SyncIO is supposed to close the underlying fd when it itself is closed
    def test_openclose_stringio(self):
        lower = io.StringIO()
        upper = SyncIO(lower)
        upper.close()
        self.assertTrue(lower.closed)

    def test_openclose_bytesio(self):
        lower = io.BytesIO()
        upper = SyncIO(lower)
        upper.close()
        self.assertTrue(lower.closed)

    def test_openclose_tempfile(self):
        lower = tempfile.TemporaryFile()
        upper = SyncIO(lower)
        upper.close()
        self.assertTrue(lower.closed)

    def test_openclose_pipe(self):
        lower_r, lower_w = os.pipe()
        os.close(lower_w)
        upper = SyncIO(lower_r, mode="rb")
        upper.close()
        with self.assertRaises(OSError) as error:
            os.close(lower_r)
        self.assertEqual(error.exception.errno, errno.EBADF)

    def test_openclose_socket(self):
        lower = socket.socket()
        upper = SyncIO(lower)
        upper.close()
        # socket's fileno() returns -1 when the socket is closed
        self.assertEqual(lower.fileno(), -1)

    def test_write_stringio(self):
        strio = io.StringIO()
        with SyncIO(strio, "r+") as sio:
            sio.write("watermelon\n")
            strio.seek(0)
            self.assertEqual(strio.read(), "watermelon\n")

    def test_read_stringio(self):
        strio = io.StringIO("pineapple\n")
        strio.seek(0)
        with SyncIO(strio, "r+") as sio:
            self.assertEqual(sio.read(), "pineapple\n")

    def test_write_bytesio(self):
        strio = io.BytesIO()
        with SyncIO(strio, "wb") as sio:
            sio.write(b"watermelon\n")
            strio.seek(0)
            self.assertEqual(strio.read(), b"watermelon\n")

    def test_read_bytesio(self):
        strio = io.BytesIO(b"pineapple\n")
        strio.seek(0)
        with SyncIO(strio, "rb") as sio:
            self.assertEqual(sio.read(), b"pineapple\n")
            strio.write(b"watermelon\n")
            strio.seek(len(b"pineapple\n"))
            self.assertEqual(sio.read(), b"watermelon\n")
            # Not getting stuck on an in-memory IO.
            self.assertEqual(sio.read(), b"")

    def test_write_socketpair(self):
        sock1, sock2 = socket.socketpair()
        sock2.settimeout(0.1)  # ideally this should be 0
        with SyncIO(sock1, "r+b") as sio:
            sio.write(b"watermelon\n")
            sio.flush()
            self.assertEqual(sock2.recv(1024), b"watermelon\n")
            sio.write(b"pineapple\n")
            sio.flush()
            self.assertEqual(sock2.recv(1024), b"pineapple\n")

    def test_broken_socket(self):
        sock1, sock2 = socket.socketpair()
        with SyncIO(sock1, "rb") as reader:
            sock2.close()
            self.assertRaises(EOFError, reader.read)
