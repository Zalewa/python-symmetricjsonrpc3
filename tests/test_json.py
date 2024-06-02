import unittest

from symmetricjsonrpc3.json import (Reader, Tokenizer, Writer,
                                    from_json, to_json)


class TestJson(unittest.TestCase):
    import socket
    import tempfile
    import threading

    def assertReadEqual(self, str, obj):
        reader = Reader(str)
        read_obj = reader.read_value()
        self.assertEqual(obj, read_obj)
        io = self.tempfile.TemporaryFile()
        Writer(io).write_value(obj)
        io.seek(0)
        reader1 = Reader(io)
        read_obj1 = reader1.read_value()
        self.assertEqual(obj, read_obj1)

    def assertWriteEqual(self, str, obj):
        self.assertEqual(str, to_json(obj))

    def test_to_json(self):
        STR = '["string",false,null]'
        OBJ = [u"string", False, None]
        self.assertEqual(to_json(OBJ), STR)

    def test_from_json(self):
        STR = '{"array": ["string",false,null],"object":{"number":4711,"bool":true}}'
        OBJ = {u"array": [u"string", False, None], u"object": {u"number": 4711, u"bool": True}}
        self.assertEqual(from_json(STR), OBJ)

    def test_single_number_from_json(self):
        STR = '3.33'
        OBJ = 3.33
        self.assertEqual(from_json(STR), OBJ)

    def test_read_value(self):
        STR = '{"array": ["string",false,null],"object":{"number":4711,"bool":true}}'
        OBJ = {u"array": [u"string", False, None], u"object": {u"number": 4711, u"bool": True}}
        self.assertReadEqual(STR, OBJ)

    def test_read_numbers(self):
        STR = '[0, -1, 0.2, 1e+4, -2.5E-5, 1e20]'
        self.assertReadEqual(STR, eval(STR))

    def test_read_escape_string(self):
        STR = r'"\b\f\n\r\t\u1234"'
        OBJ = u"\b\f\n\r\t\u1234"
        self.assertReadEqual(STR, OBJ)

    def test_read_quote_string(self):
        STR = r'"\""'
        OBJ = u"\""
        self.assertReadEqual(STR, OBJ)

    def test_read_solidus_string(self):
        STR = r'"\/"'
        OBJ = u"/"
        self.assertReadEqual(STR, OBJ)

    def test_read_reverse_solidus_string(self):
        STR = r'"\\"'
        OBJ = u"\\"
        self.assertReadEqual(STR, OBJ)

    def test_read_whitespace(self):
        STR = ''' {
"array" : [ ] ,
"object" : { }
} '''
        self.assertReadEqual(STR, eval(STR))

    def test_read_values(self):
        STR = "{}[]true false null"
        reader = Reader(STR)
        values = [{}, [], True, False, None]

        for i, r in enumerate(reader.read_values()):
            self.assertEqual(r, values[i])

    def test_encode_invalid_control_character(self):
        self.assertRaises(Exception, lambda: json('\x00', self.tempfile.TemporaryFile()))

    def test_encode_invalid_object(self):
        self.assertRaises(Exception, lambda: json(Tokenizer(""), self.tempfile.TemporaryFile()))

    def test_read_object(self):
        STR = '{"__jsonclass__":["foo","bar"],"naja":123}'
        def foo(arg, kw):
            assert arg == ["bar"]
            assert kw == {"naja":123}
            return True
        reader = Reader(STR, {'foo': foo})
        assert reader.read_value() is True

    def test_broken_socket(self):
        sockets = self.socket.socketpair()
        reader = Reader(sockets[0])
        sockets[0].close()
        self.assertRaises(self.socket.error, lambda: reader.read_value())

    def test_eof(self):
        obj = {'foo':1, 'bar':[1, 2]}
        io0 = self.tempfile.TemporaryFile()
        Writer(io0).write_value(obj)
        io0.seek(0)
        full_json_string = io0.read()

        for json_string, eof_error in ((full_json_string, False), (full_json_string[0:10], True), ('', True)):
            io1 = self.tempfile.TemporaryFile()
            io1.write(json_string)
            io1.seek(0)
            reader = Reader(io1)
            if eof_error:
                self.assertRaises(EOFError, lambda: reader.read_value())
            else:
                self.assertEqual(obj, reader.read_value())

    def test_closed_socket(self):
        class Timeout(self.threading.Thread):
            def run(self1):
                obj = {'foo':1, 'bar':[1, 2]}
                io = self.tempfile.TemporaryFile()
                Writer(io).write_value(obj)
                io.seek(0)
                full_json_string = io.read()

                for json_string, eof_error in ((full_json_string, False), (full_json_string[0:10], True), ('', True)):
                    sockets = self.socket.socketpair()
                    reader = Reader(sockets[0])

                    for c in json_string:
                        while not sockets[1].send(c): pass
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
                return {'__jsonclass__': ['SomeObj'], 'x': self.x}

        self.assertWriteEqual('{"x":4711,"__jsonclass__":["SomeObj"]}', SomeObj(4711))
