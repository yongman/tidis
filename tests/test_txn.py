#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 yongman <yming0221@gmail.com>
#
# Distributed under terms of the MIT license.

"""
unit test for transaction type
"""

import unittest
import time
from rediswrap import RedisWrapper

class TxnTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'connect to 127.0.0.1:5379\n'
        cls.r = RedisWrapper('127.0.0.1', 5379).get_instance()
        cls.k1 = '__string1__'
        cls.v1 = 'value1'
        cls.k2 = '__string2__'
        cls.v2 = 'value2'
        cls.bitPos = 0
        cls.bitVal = 1

    def setUp(self):
        self.r.delete(self.k1)
        self.r.delete(self.k2)
        pass

    def test_multi_empty(self):
        self.assertEqual(self.r.execute_command('multi'), 'OK')
        self.assertEqual(self.r.execute_command('exec'), None)
        self.assertEqual(self.r.execute_command('multi'), 'OK')
        self.assertEqual(self.r.execute_command('discard'), 'OK')

    def test_exec_without_multi(self):
        try:
            self.r.execute_command('exec')
        except BaseException,e:
            self.assertEqual(e.message, 'EXEC without MULTI')

    def test_discard_without_multi(self):
        try:
            self.r.execute_command('discard')
        except BaseException,e:
            self.assertEqual(e.message, 'DISCARD without MULTI')

    def test_exec(self):
        self.assertEqual(self.r.execute_command('multi'), 'OK')
        self.assertEqual(self.r.execute_command('set', self.k1, self.v1), 'QUEUED')
        self.assertEqual(self.r.execute_command('get', self.k1), 'QUEUED')
        self.assertEqual(self.r.execute_command('exec'), ['OK', self.v1])

    def test_discard(self):
        self.assertEqual(self.r.execute_command('multi'), 'OK')
        self.assertEqual(self.r.execute_command('set', self.k1, self.v1), 'QUEUED')
        self.assertEqual(self.r.execute_command('get', self.k1), 'QUEUED')
        self.assertEqual(self.r.execute_command('discard'), 'OK')

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.delete(cls.k1)
        cls.r.delete(cls.k2)
        print '\nclean up\n'
