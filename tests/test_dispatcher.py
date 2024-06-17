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
import unittest
from threading import Thread

from symmetricjsonrpc3.dispatcher import Count


class TestCount(unittest.TestCase):
    def test_count_value(self):
        self.assertEqual(Count(0).value, 0)
        self.assertEqual(Count(-50).value, -50)
        self.assertEqual(Count(1337).value, 1337)

    def test_count_next(self):
        count = Count()
        self.assertEqual(next(count), 0)
        self.assertEqual(next(count), 1)
        self.assertEqual(next(count), 2)

    def test_count_multithreaded(self):
        NTHREADS = 10
        count = Count()
        thrs = [Thread(name=f"test-count-multithreaded-{n}",
                       target=next, args=(count,),
                       daemon=True)
                for n in range(NTHREADS)]
        for thr in thrs:
            thr.start()
        for thr in thrs:
            thr.join(timeout=1.0)
        self.assertFalse(any(thr.is_alive() for thr in thrs))
        self.assertEqual(count.value, NTHREADS)


if __name__ == "__main__":
    unittest.main()
