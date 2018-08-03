#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 yongman <yming0221@gmail.com>
#
# Distributed under terms of the MIT license.

"""
unit test for list type
"""

import unittest
import time
import string
import random
from rediswrap import RedisWrapper

class ListTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'connect to 127.0.0.1:5379\n'
        cls.r = RedisWrapper('127.0.0.1', 5379).get_instance()
        cls.k1 = '__list1__'
        cls.k2 = '__list2__'
        cls.v1 = 'value1'
        cls.v2 = 'value2'

    def setUp(self):
        self.r.execute_command('ldel', self.k1)
        self.r.execute_command('ldel', self.k2)
        pass

    def random_string(n):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))

    def test_lpop(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.r.lpop(self.k1), str(i))

    def test_lpush(self):
        for i in range(200):
            self.assertTrue(self.r.lpush(self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.r.rpop(self.k1), str(i))

    def test_rpop(self):
        for i in range(200):
            self.assertTrue(self.r.lpush(self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.r.rpop(self.k1), str(i))

    def test_rpush(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.r.lpop(self.k1), str(i))

    def test_llen(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        self.assertEqual(self.r.llen(self.k1), 200)

    def test_lindex(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.r.lindex(self.k1, i), str(i))

    def test_lrange(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        self.assertListEqual(self.r.lrange(self.k1, 10, 100), [str(i) for i in range(10, 101)])

    def test_lset(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        self.assertTrue(self.r.lset(self.k1, 100, 'hello'))
        self.assertEqual(self.r.lindex(self.k1, 100), 'hello')

    def test_ltrim(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        self.assertTrue(self.r.ltrim(self.k1, 0, 100))
        self.assertListEqual(self.r.lrange(self.k1, 0, -1), [str(i) for i in range(0, 101)])
        self.assertEqual(self.r.llen(self.k1), 101)

    def test_ldel(self):
        for i in range(200):
            self.assertTrue(self.r.rpush(self.k1, str(i)))
        self.assertEqual(self.r.execute_command('ldel', self.k1), 1)

    def test_lpexpire(self):
        self.assertTrue(self.r.lpush(self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.r.execute_command('lpexpire', self.k1, 5000))
        self.assertLessEqual(self.r.execute_command('lpttl', self.k1), 5000)
        self.assertEqual(self.r.llen(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.llen(self.k1), 0)

    def test_lpexpireat(self):
        self.assertTrue(self.r.lpush(self.k1, self.v1))
        # expire in 5s
        ts = int(round(time.time()*1000)) + 5000
        self.assertTrue(self.r.execute_command('lpexpireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('lpttl', self.k1), 5000)
        self.assertEqual(self.r.llen(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.llen(self.k1), 0)

    def test_lexpire(self):
        self.assertTrue(self.r.lpush(self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.r.execute_command('lexpire', self.k1, 5))
        self.assertLessEqual(self.r.execute_command('lttl', self.k1), 5)
        self.assertEqual(self.r.llen(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.llen(self.k1), 0)

    def test_lexpireat(self):
        self.assertTrue(self.r.lpush(self.k1, self.v1))
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertTrue(self.r.execute_command('lexpireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('lttl', self.k1), 5)
        self.assertEqual(self.r.llen(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.llen(self.k1), 0)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('ldel', cls.k1)
        cls.r.execute_command('ldel', cls.k2)
        print '\nclean up\n'
