#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=UTF-8 :

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

"""JSON (de)serialization facilities."""
import json
import selectors
from abc import ABC, abstractmethod

from .io import Closable


def from_json(str):
    """Return a Python object representing the JSON value in str."""
    return json.loads(str)


def to_json(obj):
    """Return a compact JSON string representing the Python object obj."""
    return JSONEncoder().encode(obj)


class JSONEncoder(json.JSONEncoder):
    """Customized json.JSONEncoder.

    1. It produces compact JSON.

    2. It supports "__to_json__" dunder method that allows to serialize
       objects into JSON dumpables.

    """

    def __init__(self, *args, **kwargs):
        if kwargs.get("separators") is None:
            kwargs["separators"] = (",", ":")
        super().__init__(*args, **kwargs)

    def default(self, o):
        if hasattr(o, "__to_json__"):
            return o.__to_json__()
        return super().default(o)


class _BufferedWriter:
    MAXLEN = 8192

    def __init__(self, s):
        self.s = s
        self.buffer = ''

    def write(self, data):
        self.buffer += data
        if len(self.buffer) > self.MAXLEN:
            self.flush()

    def flush(self):
        while self.buffer:
            nwritten = self.s.write(self.buffer)
            self.s.flush()
            if nwritten == len(self.buffer):
                self.buffer = ""
            else:
                self.buffer = self.buffer[nwritten:]


class Writer(Closable):
    """A serializer for Python values to JSON.

    Allowed types for values to serialize are:

        * None
        * True
        * False
        * int
        * float
        * str
        * bytes
        * list
        * dict (keys must be str)
        * any object with a __to_json__ method

    The Writer must be instantiated with a file-like object to write
    the serialized JSON to as sole argument. To actually serialize
    data, call the write_value() method.

    No assumptions are made over the file-like, and while the Writer
    itself can be close()-d, it doesn't close the file-like.
    """

    def __init__(self, s):
        self.s = s

    def write_value(self, value):
        writebuffer = _BufferedWriter(self.s)
        json.dump(value, writebuffer, cls=JSONEncoder)
        writebuffer.flush()


class Reader(Closable):
    """A JSON parser that parses JSON strings read from a file-like.

    JSON is parsed into Python values. The file-like may provide
    multiple documents as long as they appear one after another.

    The parser must be instantiated with the file-like object. To parse
    values, call either the read_value() method, or iterate over the
    return value of the read_values() method.

    No assumptions are made over the file-like, and while the Reader
    itself can be close()-d, it doesn't close the file-like.
    """

    def __init__(self, s):
        self.s = s
        try:
            self._selector = selectors.DefaultSelector()
            self._selector.register(self.s, selectors.EVENT_READ)
        except (PermissionError, ValueError):
            # not a real file
            self._selector = None
        self._eof = None
        self._decoder = JSONDecoderBuffer()
        self._closed = False

    def read_value(self):
        if self._decoder.has_decoded():
            return self._decoder.pop()
        if self._eof:
            raise self._eof

        while not self._eof:
            if self._selector:
                events = self._selector.select(timeout=0.1)
            else:
                events = True
            if self._closed:
                chunk = None
            elif events:
                chunk = self.s.read(4096)
            else:
                continue
            if chunk:
                if self._decoder.eat(chunk):
                    return self._decoder.pop()
            else:
                self._eof = EOFError()
                try:
                    if self._decoder.flush():
                        return self._decoder.pop()
                except json.JSONDecodeError:
                    pass
        raise self._eof

    def read_values(self):
        try:
            while True:
                yield self.read_value()
        except EOFError:
            return

    def close(self):
        self._closed = True


class JSONScanner(ABC):
    """Scan text looking for JSON documents.

    JSONScanner can be used to extract a JSON document from a larger
    file (or any input stream), without actually parsing the JSON into
    Python objects.

    In other words: this scanner doesn't *decode* the raw JSON, it only
    looks at raw text and tries to find where a complete document is
    located in that text.

    It allows the text to be provided in chunks, e.g. when you're
    downloading a large JSON from the web, and the total file size is
    unknown.

    Usage:

    1. First, open() the appropriate scanner subtype for your document
       (be it an array, an object or a primitive).

    2. Then feed the incoming JSON data into scan().

    3. scan() returns the length of the document as soon as it detects
       its end, or keeps returning None as long as it deems that the
       document is still going on.

    If the document ends due to an error (ends prematurely), the
    position at which the error occurred is returned.

    In consequence, the scanner itself doesn't say if the document is
    correct. Its job is to extract JSON documents from ongoing streams.
    So, it extracts slices from the string that *look* like they
    could be proper documents. These slices can then be passed to the
    actual parser. The actual parser may fail afterwards, if the slice
    doesn't actually describe a proper document.

    The position of the current (ongoing) scan can always be retrieved
    from the `pos` attribute.

    A JSONScanner is single-use only. It assumes it starts at the
    beginning of the document. Once it finds its end, or fails with an
    error, it cannot be used to find another document anymore. To scan
    for another document, even in the same text, you must open() a new
    scanner.

    """

    def __init__(self):
        self.pos = 0
        self._first_symbol_pos = None

    def scan(self, batch):
        if self._first_symbol_pos is None:
            batch = self._eat_whitespace(batch)
        return self._scan(batch)

    @abstractmethod
    def _scan(self, batch): pass

    @classmethod
    def open(cls, batch):
        """Open the appropriate scanner for reading a new document.

        If `None` is returned it means that the batch was all whitespace
        and can be discarded.

        """
        batch = batch.lstrip()
        if not batch:
            return None

        c = batch[0]
        if c == '[':
            return JSONArrayScanner()
        elif c == '{':
            return JSONObjectScanner()
        elif c == '"':
            return JSONStringScanner()
        elif c == 'f':
            return JSONLiteralScanner('false')
        elif c == 'n':
            return JSONLiteralScanner('null')
        elif c == 't':
            return JSONLiteralScanner('true')
        else:
            return JSONNumberScanner()

    def _eat_whitespace(self, batch):
        idx = -1
        for idx, c in enumerate(batch):
            if c == ' ':
                self.pos += 1
            else:
                self._first_symbol_pos = self.pos
                break
        return batch[idx:]


class JSONLiteralScanner(JSONScanner):
    def __init__(self, literal):
        super().__init__()
        self.literal = literal
        self._scan_pos = 0

    def _scan(self, batch):
        for c in batch:
            if (self._scan_pos == len(self.literal)
                    or c != self.literal[self._scan_pos]):
                return self.pos
            self.pos += 1
            self._scan_pos += 1
        return self.pos if self._scan_pos == len(self.literal) else None


class JSONNumberScanner(JSONScanner):
    def __init__(self):
        super().__init__()
        self._scan_pos = None
        self._phase_scan = self._scan_begin
        self._found_first_digit = False
        self._e_pos = None

    def _scan(self, batch):
        pos = None
        while batch and pos is None:
            pos, batch = self._phase_scan(batch)
        return pos

    def _scan_begin(self, batch):
        self.pos += 1
        self._scan_pos = self.pos
        self._phase_scan = self._scan_integer
        c = batch[0]
        if c == "-" or c.isdigit():
            self._found_first_digit = c.isdigit()
            batch = batch[1:]
            return None, batch
        else:
            return self.pos, ''

    def _scan_integer(self, batch):
        for idx, c in enumerate(batch):
            self._scan_pos += 1
            if c == ".":
                self._phase_scan = self._scan_fraction
                if self._found_first_digit:
                    return None, batch[idx + 1:]
                else:
                    self.pos = self._scan_pos
                    return self.pos, ''
            elif c.isdigit():
                self._found_first_digit = True
                self.pos = self._scan_pos
            elif c in 'eE':
                self._e_pos = self._scan_pos
                self._phase_scan = self._scan_scientific
                if self._found_first_digit:
                    return None, batch[idx + 1:]
                else:
                    return self.pos, ''
            else:
                return self.pos, ''
        return None, ''

    def _scan_fraction(self, batch):
        for idx, c in enumerate(batch):
            self._scan_pos += 1
            if c.isdigit():
                self.pos = self._scan_pos
            elif c in 'eE':
                self._e_pos = self._scan_pos
                self._phase_scan = self._scan_scientific
                return None, batch[idx + 1:]
            else:
                return self.pos, ''
        return None, ''

    def _scan_scientific(self, batch):
        for c in batch:
            self._scan_pos += 1
            if c.isdigit():
                self.pos = self._scan_pos
            elif self._scan_pos == self._e_pos + 1 and c in '+-':
                pass
            else:
                return self.pos, ''
        return None, ''


class JSONStringScanner(JSONScanner):
    def __init__(self):
        super().__init__()
        self._open = False
        self._escape = False

    def _scan(self, batch):
        if batch:
            if self.pos == self._first_symbol_pos:
                self.pos += 1
                if batch[0] != '"':
                    return self.pos
                else:
                    batch = batch[1:]
            for c in batch:
                self.pos += 1
                if self._escape:
                    self._escape = False
                    continue

                if c == '\\':
                    self._escape = True
                elif c == '"' or ord(c) < 0x20:
                    return self.pos
        return None


class JSONComplexScanner(JSONScanner):
    """Base class for scanning for arrays or objects.

    The "complex" scanner is actually primitive in the sense that it
    doesn't care about the internal validity of the object. It knows
    the opening symbol (either '{' or '[') and scans the document until
    it finds its closing equivalent ('}' or ']'). Therefore, a malformed
    document that doesn't have the closing symbol will be scanned
    indefinitely.

    """

    SEEKING = None

    def __init__(self):
        super().__init__()
        self._count = 0
        self._in_string = False
        self._in_escape = False

    def _scan(self, batch):
        if not batch:
            return None
        for c in batch:
            self.pos += 1
            if self._in_string:
                if self._in_escape:
                    self._in_escape = False
                elif c == '\\':
                    self._in_escape = True
                elif c == '"':
                    self._in_string = False
                continue

            if c == '"':
                self._in_string = True
            elif c == self.SEEKING[0]:
                self._count += 1
            elif c == self.SEEKING[1]:
                self._count -= 1
            if self._count == 0:
                return self.pos
        return None

    def flush(self):
        if self._buffered:
            to_flush = self._buffered
            self._buffered = ""
            self._decoded.append(json.loads(to_flush))
        return len(self._decoded)


class JSONArrayScanner(JSONComplexScanner):
    """JSONComplexScanner for arrays."""
    SEEKING = ('[', ']')


class JSONObjectScanner(JSONComplexScanner):
    """JSONComplexScanner for objects."""
    SEEKING = ('{', '}')


class JSONDecoderBuffer:
    """Buffer for decoding continuously incoming JSON values.

    It collects the incoming raw data in eat(), where it employs
    JSONScanner to slice it into JSON documents. Then it decodes the
    documents into Python objects that can then be later accessed with
    pop().

    Use has_decoded() to check if the buffer has any decoded objects
    pending a pop().

    Once the input stream is closed, but you still wish to decode the
    potentially remaining buffered raw data, call flush().

    """

    def __init__(self):
        self._buffered = ""
        self._decoded = []
        self._scanner = None

    def eat(self, batch):
        self._buffered += batch
        while self._buffered:
            if self._scanner is None:
                # A new document always begins at the beginning of the collected buffer.
                self._scanner = JSONScanner.open(self._buffered)
                if self._scanner is None:
                    # It was all whitespace.
                    self._buffered = ""
                    break
                doclen = self._scanner.scan(self._buffered)
            else:
                # An ongoing document must only scan the newly arrived batch.
                doclen = self._scanner.scan(batch)

            if doclen is None:
                # Must eat more.
                break

            # Document detected. Reset the scanner, then decode it.
            self._scanner = None
            to_decode = self._buffered[:doclen]
            self._buffered = self._buffered[doclen:]
            self._decoded.append(json.loads(to_decode))

        return len(self._decoded)

    def flush(self):
        """Flush the buffer; do this at the end of the input stream.

        It will attempt to decode the remaining buffer, if any.

        """
        if self._scanner:
            doclen = self._scanner.pos
            self._scanner = None
            to_decode = self._buffered[:doclen]
            self._buffered = ""
            if isinstance(self._scanner, JSONComplexScanner):
                # If object is being read but was not completed
                # at this point, we've ran out of the input stream.
                raise EOFError
            else:
                self._decoded.append(json.loads(to_decode))
        return len(self._decoded)

    def pop(self):
        return self._decoded.pop(0)

    def has_decoded(self):
        return bool(self._decoded)
