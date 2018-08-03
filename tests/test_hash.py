#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 yongman <yming0221@gmail.com>
#
# Distributed under terms of the MIT license.

"""
unit test for hash type
"""

import unittest
import time
from rediswrap import RedisWrapper

class HashTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'connect to 127.0.0.1:5379\n'
        cls.r = RedisWrapper('127.0.0.1', 5379).get_instance()
        cls.k1 = '__hash1__'
        cls.k2 = '__hash2__'

        cls.f1 = 'f1'
        cls.f2 = 'f2'
        cls.f3 = 'f3'
        cls.f4 = 'f4'

        cls.v1 = 'value1'
        cls.v2 = 'value2'
        cls.v3 = 'value3'
        cls.v4 = 'value4'

    def setUp(self):
        self.r.execute_command('hclear', self.k1)
        self.r.execute_command('hclear', self.k2)
        pass

    def test_hget(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.v1, self.r.hget(self.k1, self.f1))

    def test_hset(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.v1, self.r.hget(self.k1, self.f1))

    def test_hexists(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertTrue(self.r.hexists(self.k1, self.f1))

    def test_hstrlen(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.r.hstrlen(self.k1, self.f1), len(self.v1))

    def test_hlen(self):
        prefix = '__'
        for i in range(0, 200):
            f = '{}{}'.format(prefix, i)
            self.assertEqual(self.r.hset(self.k2, f, f), 1)
        self.assertEqual(self.r.hlen(self.k2), 200)

    def test_hmget(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        self.assertListEqual(self.r.hmget(self.k1, self.f1, self.f2, self.f3), [self.v1, self.v2, self.v3])

    def test_hdel(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        self.assertEqual(self.r.hdel(self.k1, self.f1, self.f2, self.f3, self.f4), 3)
        self.assertEqual(self.r.hlen(self.k1), 0)

        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        self.assertEqual(self.r.hdel(self.k1, self.f1, self.f2), 2)
        self.assertEqual(self.r.hlen(self.k1), 1)

    def test_hkeys(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        self.assertListEqual(self.r.hkeys(self.k1), [self.f1, self.f2, self.f3])

    def test_hvals(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        self.assertListEqual(self.r.hvals(self.k1), [self.v1, self.v2, self.v3])

    def test_hgetall(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        self.assertDictEqual(self.r.hgetall(self.k1), {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3})

    def test_hclear(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        self.assertTrue(self.r.execute_command("HCLEAR", self.k1))
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_hpexpire(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        # expire in 5s
        self.assertEqual(self.r.execute_command("HPEXPIRE", self.k1, 5000), 1)
        self.assertLessEqual(self.r.execute_command("HPTTL", self.k1), 5000)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_hpexpireat(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        # expire in 5s
        ts = int(round(time.time()*1000)) + 5000
        self.assertEqual(self.r.execute_command('hpexpireat', self.k1, ts), 1)
        self.assertLessEqual(self.r.execute_command('hpttl', self.k1), 5000)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_hexpire(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        # expire in 5s
        self.assertEqual(self.r.execute_command('hexpire', self.k1, 5), 1)
        self.assertLessEqual(self.r.execute_command('httl', self.k1), 5)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_hexpireat(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1:self.v1, self.f2:self.v2, self.f3:self.v3}))
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertEqual(self.r.execute_command('hexpireat', self.k1, ts), 1)
        self.assertLessEqual(self.r.execute_command('httl', self.k1), 5)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('hclear', cls.k1)
        cls.r.execute_command('hclear', cls.k2)
        print '\nclean up\n'
