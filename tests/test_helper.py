#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 yongman <yming0221@gmail.com>
#
# Distributed under terms of the MIT license.

"""
all unit tests
"""

import unittest

from test_string import StringTest
from test_hash import HashTest
from test_list import ListTest
from test_set import SetTest
from test_zset import ZsetTest

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(StringTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(HashTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ListTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SetTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ZsetTest))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
