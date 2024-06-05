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
import json
import socket
import tempfile
import threading
import unittest
from collections import OrderedDict

from symmetricjsonrpc3.json import Reader, Writer, from_json, to_json, \
    JSONDecoderBuffer, JSONScanner, JSONLiteralScanner, JSONNumberScanner, \
    JSONStringScanner, JSONArrayScanner, JSONObjectScanner


class TestJson(unittest.TestCase):
    def assertReadEqual(self, str, obj):
        reader = Reader(str)
        read_obj = reader.read_value()
        self.assertEqual(obj, read_obj)
        with tempfile.TemporaryFile("w+") as io:
            Writer(io).write_value(obj)
            io.flush()
            io.seek(0)
            reader1 = Reader(io)
            read_obj1 = reader1.read_value()
            self.assertEqual(obj, read_obj1)

    def assertWriteEqual(self, str, obj):
        self.assertEqual(str, to_json(obj))

    def test_to_json(self):
        STR = '["string",false,null]'
        OBJ = ["string", False, None]
        self.assertEqual(to_json(OBJ), STR)

    def test_from_json(self):
        STR = '{"array": ["string",false,null],"object":{"number":4711,"bool":true}}'
        OBJ = {"array": ["string", False, None], "object": {"number": 4711, "bool": True}}
        self.assertEqual(from_json(STR), OBJ)

    def test_single_number_from_json(self):
        STR = '3.33'
        OBJ = 3.33
        self.assertEqual(from_json(STR), OBJ)

    def test_read_value(self):
        STR = '{"array": ["string",false,null],"object":{"number":4711,"bool":true}}'
        OBJ = {"array": ["string", False, None], "object": {"number": 4711, "bool": True}}
        self.assertReadEqual(STR, OBJ)

    def test_read_numbers(self):
        STR = '[0, -1, 0.2, 1e+4, -2.5E-5, 1e20]'
        self.assertReadEqual(STR, eval(STR))

    def test_read_escape_string(self):
        STR = r'"\b\f\n\r\t\u1234"'
        OBJ = "\b\f\n\r\t\u1234"
        self.assertReadEqual(STR, OBJ)

    def test_read_quote_string(self):
        STR = r'"\""'
        OBJ = "\""
        self.assertReadEqual(STR, OBJ)

    def test_read_solidus_string(self):
        STR = r'"\/"'
        OBJ = "/"
        self.assertReadEqual(STR, OBJ)

    def test_read_reverse_solidus_string(self):
        STR = r'"\\"'
        OBJ = "\\"
        self.assertReadEqual(STR, OBJ)

    def test_read_whitespace(self):
        STR = ''' {
"array" : [ ] ,
"object" : { }
} '''
        self.assertReadEqual(STR, json.loads(STR))

    def test_read_values(self):
        STR = "{}[]true false null"
        reader = Reader(STR)
        values = [{}, [], True, False, None]

        for i, r in enumerate(reader.read_values()):
            self.assertEqual(r, values[i])

    def test_encode_invalid_object(self):
        self.assertRaises(TypeError, lambda: to_json(object))

    def test_broken_socket(self):
        sockets = socket.socketpair()
        reader = Reader(sockets[0])
        sockets[0].close()
        self.assertRaises(socket.error, lambda: reader.read_value())

    def test_eof(self):
        obj = {'foo': 1, 'bar': [1, 2]}
        full_json_string = json.dumps(obj)

        for json_string, eof_error in [(full_json_string, False),
                                       (full_json_string[:10], True),
                                       ('', True)]:
            with tempfile.TemporaryFile("w+") as io:
                io.write(json_string)
                io.flush()
                io.seek(0)
                reader = Reader(io)
                if eof_error:
                    self.assertRaises(EOFError, lambda: reader.read_value())
                else:
                    self.assertEqual(obj, reader.read_value())

    def test_closed_socket(self):
        class Timeout(threading.Thread):
            def run(self1):
                obj = {'foo': 1, 'bar': [1, 2]}
                full_json_string = json.dumps(obj)
                for json_string, eof_error in [(full_json_string, False),
                                               (full_json_string[:10], True),
                                               ('', True)]:
                    sockets = socket.socketpair()
                    reader = Reader(sockets[0])

                    for c in json_string:
                        while not sockets[1].send(c.encode('ascii')):
                            pass
                    sockets[1].close()
                    if eof_error:
                        self.assertRaises(EOFError, lambda: reader.read_value())
                    else:
                        self.assertEqual(obj, reader.read_value())

        timeout = Timeout()
        timeout.start()
        timeout.join(3)
        if timeout.is_alive():
            self.fail('Reader has hung.')

    def test_write_object(self):
        class SomeObj:
            def __init__(self, x):
                self.x = x

            def __to_json__(self):
                return OrderedDict([('x', self.x), ('__jsonclass__', ['SomeObj'])])

        self.assertWriteEqual('{"x":4711,"__jsonclass__":["SomeObj"]}', SomeObj(4711))


class TestJSONScanner(unittest.TestCase):
    def test_open(self):
        self.assertIsNone(JSONScanner.open(" "))
        self.assertIsInstance(JSONScanner.open("["), JSONArrayScanner)
        self.assertIsInstance(JSONScanner.open(" ["), JSONArrayScanner)
        self.assertIsInstance(JSONScanner.open("f"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open("false"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open(" false"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open("n"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open("null"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open(" null"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open("t"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open("true"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open(" true"), JSONLiteralScanner)
        self.assertIsInstance(JSONScanner.open("1"), JSONNumberScanner)
        self.assertIsInstance(JSONScanner.open(" 1"), JSONNumberScanner)
        self.assertIsInstance(JSONScanner.open("-"), JSONNumberScanner)
        self.assertIsInstance(JSONScanner.open(" -"), JSONNumberScanner)
        self.assertIsInstance(JSONScanner.open("{"), JSONObjectScanner)
        self.assertIsInstance(JSONScanner.open(" {"), JSONObjectScanner)
        self.assertIsInstance(JSONScanner.open('"'), JSONStringScanner)
        self.assertIsInstance(JSONScanner.open(' "'), JSONStringScanner)


class TestJSONLiteralScanner(unittest.TestCase):
    def setUp(self):
        self.jsonscanner = JSONLiteralScanner("true")

    # Valids
    def test_scan_literal(self):
        self.assertEqual(self.jsonscanner.scan("true"), 4)

    def test_scan_literal_ended_ok(self):
        self.assertEqual(self.jsonscanner.scan("true "), 4)

    def test_scan_literal_extraneous(self):
        # This case looks weird, but all the scanners process JSON in chunks
        # with assumptions that the streams are continuous and multi-document.
        # They can't know if the literal arrives in a completely valid
        # "tr" + "ue" chunk pair, an invalid "true" + "flish" pair, or a
        # completely valid singular "true" chunk that won't have anything
        # following in a while (or ever).
        #
        # JSON-RPC doesn't transfer plain literals anyway, so it's best not
        # to think too hard about this.
        self.assertEqual(self.jsonscanner.scan("trueflish"), 4)

    def test_scan_literal_too_short(self):
        self.assertIsNone(self.jsonscanner.scan("tru"))
        self.assertEqual(self.jsonscanner.pos, 3)

    def test_scan_leading_whitespace(self):
        self.assertEqual(self.jsonscanner.scan("  true "), 6)

    # Invalids
    def test_scan_literal_invalid(self):
        self.assertEqual(self.jsonscanner.scan("trut "), 3)

    def test_scan_literal_very_invalid(self):
        self.assertEqual(self.jsonscanner.scan("fruit "), 0)


class TestJSONNumberScanner(unittest.TestCase):
    def setUp(self):
        self.jsonscanner = JSONNumberScanner()

    # Valids
    def test_scan_nothing(self):
        self.assertIsNone(self.jsonscanner.scan(""))
        self.assertEqual(self.jsonscanner.pos, 0)

    def test_scan_one(self):
        self.assertIsNone(self.jsonscanner.scan("1"))
        self.assertEqual(self.jsonscanner.pos, 1)

    def test_scan_consecutive(self):
        self.assertIsNone(self.jsonscanner.scan("1"))
        self.assertIsNone(self.jsonscanner.scan("2"))
        self.assertIsNone(self.jsonscanner.scan("3"))
        self.assertEqual(self.jsonscanner.scan("\n"), 3)

    def test_scan_longer_number(self):
        self.assertIsNone(self.jsonscanner.scan("1337"))
        self.assertEqual(self.jsonscanner.pos, 4)

    def test_scan_negative_number(self):
        self.assertIsNone(self.jsonscanner.scan("-1337"))
        self.assertEqual(self.jsonscanner.pos, 5)

    def test_scan_real(self):
        self.assertIsNone(self.jsonscanner.scan("313.37"))
        self.assertEqual(self.jsonscanner.pos, 6)

    def test_scan_negative_real(self):
        self.assertIsNone(self.jsonscanner.scan("-313.37"))
        self.assertEqual(self.jsonscanner.pos, 7)

    def test_scan_scientific_notation(self):
        Scanner = JSONNumberScanner
        self.assertEqual(Scanner().scan("1.2E10 "), 6)
        self.assertEqual(Scanner().scan("1.2e10 "), 6)
        self.assertEqual(Scanner().scan("1.2E+10 "), 7)
        self.assertEqual(Scanner().scan("1.2e+10 "), 7)
        self.assertEqual(Scanner().scan("1.2E-10 "), 7)
        self.assertEqual(Scanner().scan("1.2e-10 "), 7)
        self.assertEqual(Scanner().scan("1E10 "), 4)
        self.assertEqual(Scanner().scan("1e10 "), 4)
        self.assertEqual(Scanner().scan("1E+10 "), 5)
        self.assertEqual(Scanner().scan("1e+10 "), 5)
        self.assertEqual(Scanner().scan("1E-10 "), 5)
        self.assertEqual(Scanner().scan("1e-10 "), 5)
        self.assertEqual(Scanner().scan("-1.2E10 "), 7)
        self.assertEqual(Scanner().scan("-1.2e10 "), 7)
        self.assertEqual(Scanner().scan("-1.2E+10 "), 8)
        self.assertEqual(Scanner().scan("-1.2e+10 "), 8)
        self.assertEqual(Scanner().scan("-1.2E-10 "), 8)
        self.assertEqual(Scanner().scan("-1.2e-10 "), 8)
        self.assertEqual(Scanner().scan("-1E10 "), 5)
        self.assertEqual(Scanner().scan("-1e10 "), 5)
        self.assertEqual(Scanner().scan("-1E+10 "), 6)
        self.assertEqual(Scanner().scan("-1e+10 "), 6)
        self.assertEqual(Scanner().scan("-1E-10 "), 6)
        self.assertEqual(Scanner().scan("-1e-10 "), 6)

    def test_scan_trailing_dot(self):
        self.assertIsNone(self.jsonscanner.scan("13."))
        self.assertEqual(self.jsonscanner.pos, 2)

    def test_scan_trailing_two_dots(self):
        self.assertEqual(self.jsonscanner.scan("13.37."), 5)

    def test_scan_trailing_dot_end(self):
        self.assertEqual(self.jsonscanner.scan("13.\n"), 2)

    def test_scan_document_whitespace_end(self):
        self.assertEqual(self.jsonscanner.scan("10000\n"), 5)
        self.assertEqual(self.jsonscanner.pos, 5)

    def test_scan_leading_whitespace(self):
        Scanner = JSONNumberScanner
        self.assertEqual(Scanner().scan(" 1 "), 2)
        self.assertEqual(Scanner().scan("  1 "), 3)
        self.assertEqual(Scanner().scan("  -1 "), 4)

    # Invalids
    def test_scan_invalid(self):
        self.assertEqual(self.jsonscanner.scan("watermelon"), 1)

    def test_scan_middle_minus(self):
        self.assertEqual(self.jsonscanner.scan("13-37"), 2)

    def test_scan_dot_begin(self):
        self.assertEqual(self.jsonscanner.scan(".1"), 1)

    def test_scan_dot_end(self):
        self.assertEqual(self.jsonscanner.scan("1337. "), 4)

    def test_scan_two_dots(self):
        self.assertEqual(self.jsonscanner.scan("1.2.3"), 3)

    def test_scan_minusdot_begin(self):
        self.assertEqual(self.jsonscanner.scan("-.1"), 2)

    def test_scan_scientific_invalids(self):
        Scanner = JSONNumberScanner
        self.assertEqual(Scanner().scan("-E10 "), 1)
        self.assertEqual(Scanner().scan("E10 "), 1)
        self.assertEqual(Scanner().scan("1e1.0 "), 3)
        self.assertEqual(Scanner().scan("15e.0 "), 2)
        self.assertEqual(Scanner().scan("1.2E "), 3)
        self.assertEqual(Scanner().scan("1.2E+ "), 3)
        self.assertEqual(Scanner().scan("1.2E- "), 3)
        self.assertEqual(Scanner().scan("1.2E. "), 3)
        self.assertEqual(Scanner().scan("1.2E1.5 "), 5)
        self.assertEqual(Scanner().scan("1.2E2e2 "), 5)
        self.assertEqual(Scanner().scan("1.2E2X2 "), 5)
        self.assertEqual(Scanner().scan("1.2Ee "), 3)
        self.assertEqual(Scanner().scan("1.2EX "), 3)


class TestJSONStringScanner(unittest.TestCase):
    def setUp(self):
        self.jsonscanner = JSONStringScanner()

    # Valids
    def test_scan_nothing(self):
        self.assertIsNone(self.jsonscanner.scan(''))
        self.assertEqual(self.jsonscanner.pos, 0)

    def test_scan_invalid(self):
        self.assertEqual(self.jsonscanner.scan('1'), 1)

    def test_scan_empty(self):
        self.assertEqual(self.jsonscanner.scan('""'), 2)

    def test_scan_only_open(self):
        self.assertIsNone(self.jsonscanner.scan('"'))
        self.assertEqual(self.jsonscanner.pos, 1)

    def test_scan_open_but_no_end(self):
        self.assertIsNone(self.jsonscanner.scan('"water'))
        self.assertEqual(self.jsonscanner.pos, len('"water'))

    def test_scan_word(self):
        self.assertEqual(self.jsonscanner.scan('"watermelon"'), len('"watermelon"'))

    def test_scan_with_escaped_quote(self):
        self.assertEqual(self.jsonscanner.scan('"water\\"melon"'), len('"water\\"melon"'))

    def test_scan_leading_whitespace(self):
        Scanner = JSONStringScanner
        self.assertEqual(Scanner().scan(' "watermelon" '), len(' "watermelon"'))
        self.assertEqual(Scanner().scan('  "watermelon"  '), len('  "watermelon"'))

    # Invalids
    def test_scan_newline(self):
        self.assertEqual(self.jsonscanner.scan('"water\nmelon"'), len('"water\n'))

    def test_scan_tab(self):
        self.assertEqual(self.jsonscanner.scan('"water\tmelon"'), len('"water\t'))


class TestJSONArrayScanner(unittest.TestCase):
    def setUp(self):
        self.jsonscanner = JSONArrayScanner()

    # Valids
    def test_scan_nothing(self):
        self.assertIsNone(self.jsonscanner.scan(""))
        self.assertEqual(self.jsonscanner.pos, 0)

    def test_scan_empty(self):
        self.assertEqual(self.jsonscanner.scan("[]"), 2)

    def test_scan_simple_array(self):
        self.assertEqual(self.jsonscanner.scan("[1]"), 3)

    def test_scan_two_arrays(self):
        self.assertEqual(self.jsonscanner.scan("[1337][2]"), len("[1337]"))

    def test_scan_array_in_array(self):
        self.assertEqual(self.jsonscanner.scan("[[1337], [2, 8], 10]"), len("[[1337], [2, 8], 10]"))

    def test_scan_partial_simple_array(self):
        self.assertIsNone(self.jsonscanner.scan('['))
        self.assertEqual(self.jsonscanner.pos, 1)
        self.assertIsNone(self.jsonscanner.scan('1'))
        self.assertEqual(self.jsonscanner.pos, 2)
        self.assertEqual(self.jsonscanner.scan(']'), 3)
        self.assertEqual(self.jsonscanner.pos, 3)

    def test_scan_array_with_opener_in_string(self):
        self.assertEqual(self.jsonscanner.scan('["["]'), 5)

    def test_scan_array_with_closer_in_string(self):
        self.assertEqual(self.jsonscanner.scan('["]"]'), 5)

    def test_scan_leading_whitespace(self):
        self.assertEqual(JSONArrayScanner().scan('  [1]  '), len('  [1]'))

    # Invalids
    def test_scan_number(self):
        self.assertEqual(self.jsonscanner.scan("5"), 1)

    def test_scan_string(self):
        self.assertEqual(self.jsonscanner.scan('"abc"'), 1)

    def test_scan_malformed_array(self):
        # It keeps scanning until ']' even though the array is
        # malformed at ',' already.
        self.assertEqual(self.jsonscanner.scan('[1,]'), 4)


class TestJSONObjectScanner(unittest.TestCase):
    def setUp(self):
        self.jsonscanner = JSONObjectScanner()

    # Valids
    def test_scan_nothing(self):
        self.assertIsNone(self.jsonscanner.scan(""))
        self.assertEqual(self.jsonscanner.pos, 0)

    def test_scan_empty(self):
        self.assertEqual(self.jsonscanner.scan("{}"), 2)

    def test_scan_simple_object(self):
        self.assertEqual(self.jsonscanner.scan('{"a": 1}'), 8)

    def test_scan_two_objects(self):
        self.assertEqual(self.jsonscanner.scan('{"a": 1337}{"b": 2}'), 11)

    def test_scan_object_in_object(self):
        self.assertEqual(self.jsonscanner.scan('{"a": {"X:" 1337}}'), len('{"a": {"X:" 1337}}'))

    def test_scan_partial_simple_object(self):
        self.assertIsNone(self.jsonscanner.scan('{"a'))
        self.assertEqual(self.jsonscanner.pos, 3)
        self.assertIsNone(self.jsonscanner.scan('": '))
        self.assertEqual(self.jsonscanner.pos, 6)
        self.assertEqual(self.jsonscanner.scan('1}'), 8)
        self.assertEqual(self.jsonscanner.pos, 8)

    def test_scan_object_with_opener_in_string(self):
        self.assertEqual(self.jsonscanner.scan('{"{": 1}'), 8)

    def test_scan_object_with_closer_in_string(self):
        self.assertEqual(self.jsonscanner.scan('{"}": 2}'), 8)

    def test_scan_leading_whitespace(self):
        self.assertEqual(JSONObjectScanner().scan('  {"a" : 1}  '), len('  {"a" : 1}'))

    # Invalids
    def test_scan_number(self):
        self.assertEqual(self.jsonscanner.scan('5'), 1)

    def test_scan_string(self):
        self.assertEqual(self.jsonscanner.scan('"abc"'), 1)

    def test_scan_malformed_object(self):
        # It keeps scanning until '}' even though the object is
        # malformed at 'a' already.
        self.assertEqual(self.jsonscanner.scan('{a:1}'), 5)


class TestJSONDecoderBuffer(unittest.TestCase):
    def setUp(self):
        self.jsondecoder = JSONDecoderBuffer()

    def test_eat_nothing(self):
        self.assertEqual(0, self.jsondecoder.eat(""))

    def test_flush_nothing(self):
        self.assertEqual(0, self.jsondecoder.flush())

    def test_eat_empty_array(self):
        self.assertEqual(1, self.jsondecoder.eat("[]"))
        res = self.jsondecoder.pop()
        self.assertIsInstance(res, list)
        self.assertFalse(bool(res))

    def test_eat_empty_object(self):
        self.assertEqual(1, self.jsondecoder.eat("{}"))
        res = self.jsondecoder.pop()
        self.assertIsInstance(res, dict)
        self.assertFalse(bool(res))

    def test_eat_simple_array(self):
        self.assertEqual(1, self.jsondecoder.eat("[1]"))
        self.assertEqual([1], self.jsondecoder.pop())

    def test_eat_simple_object(self):
        self.assertEqual(1, self.jsondecoder.eat('{"a":1}'))
        self.assertEqual({"a": 1}, self.jsondecoder.pop())

    def test_eat_two_arrays(self):
        self.assertEqual(2, self.jsondecoder.eat("[1][2]"))
        a1 = self.jsondecoder.pop()
        a2 = self.jsondecoder.pop()
        self.assertEqual(a1, [1])
        self.assertEqual(a2, [2])

    def test_eat_two_objects(self):
        self.assertEqual(2, self.jsondecoder.eat('{"a": 1}{"b": 2}'))
        o1 = self.jsondecoder.pop()
        o2 = self.jsondecoder.pop()
        self.assertEqual(o1, {"a": 1})
        self.assertEqual(o2, {"b": 2})

    def test_eat_partial_simple_array(self):
        self.assertEqual(0, self.jsondecoder.eat('['))
        self.assertEqual(0, self.jsondecoder.eat('1'))
        self.assertEqual(1, self.jsondecoder.eat(']'))
        self.assertEqual([1], self.jsondecoder.pop())

    def test_eat_partial_simple_object(self):
        self.assertEqual(0, self.jsondecoder.eat('{"a'))
        self.assertEqual(1, self.jsondecoder.eat('": 1}'))
        self.assertEqual({"a": 1}, self.jsondecoder.pop())

    def test_eat_malformed_array(self):
        self.assertRaises(json.JSONDecodeError, self.jsondecoder.eat, '[1,]')

    def test_eat_malformed_object(self):
        self.assertRaises(json.JSONDecodeError, self.jsondecoder.eat, '{"a":}')

    def test_eat_array_with_opener_in_string(self):
        self.assertEqual(1, self.jsondecoder.eat('["["]'))
        self.assertEqual(["["], self.jsondecoder.pop())

    def test_eat_array_with_closer_in_string(self):
        self.assertEqual(1, self.jsondecoder.eat('["]"]'))
        self.assertEqual(["]"], self.jsondecoder.pop())

    def test_eat_object_with_opener_in_string(self):
        self.assertEqual(1, self.jsondecoder.eat('{"a": "{"}'))
        self.assertEqual({"a": "{"}, self.jsondecoder.pop())

    def test_eat_object_with_closer_in_string(self):
        self.assertEqual(1, self.jsondecoder.eat('{"a": "}"}'))
        self.assertEqual({"a": "}"}, self.jsondecoder.pop())

    def test_eat_array_with_opener_in_string_that_is_escaped(self):
        self.assertEqual(1, self.jsondecoder.eat(r'["\"["]'))
        self.assertEqual(['"['], self.jsondecoder.pop())

    def test_eat_array_with_closer_in_string_that_is_escaped(self):
        self.assertEqual(1, self.jsondecoder.eat(r'["]\""]'))
        self.assertEqual([']"'], self.jsondecoder.pop())

    def test_eat_object_with_opener_in_string_that_is_escaped(self):
        self.assertEqual(1, self.jsondecoder.eat(r'{"a": "\"{"}'))
        self.assertEqual({"a": '"{'}, self.jsondecoder.pop())

    def test_eat_object_with_closer_in_string_that_is_escaped(self):
        self.assertEqual(1, self.jsondecoder.eat(r'{"a": "}\""}'))
        self.assertEqual({"a": '}"'}, self.jsondecoder.pop())

    def test_eat_array_with_garbage_after(self):
        self.assertRaises(json.JSONDecodeError, self.jsondecoder.eat, '[1]haxme]')

    def test_eat_object_with_garbage_after(self):
        self.assertRaises(json.JSONDecodeError, self.jsondecoder.eat, '{"a": 1}haxme}')

    def test_flush_unfinished_array(self):
        self.jsondecoder.eat("[1,")
        self.assertRaises(json.JSONDecodeError, self.jsondecoder.flush)

    def test_flush_unfinished_object(self):
        self.jsondecoder.eat("{")
        self.assertRaises(json.JSONDecodeError, self.jsondecoder.flush)
