#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 yongman <yming0221@gmail.com>
#
# Distributed under terms of the MIT license.

"""
unit test for set type
"""

import unittest
import time
import string
import random
from rediswrap import RedisWrapper

class ZsetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'connect to 127.0.0.1:5379\n'
        cls.r = RedisWrapper('127.0.0.1', 5379).get_instance()
        cls.k1 = '__set1__'
        cls.k2 = '__set2__'
        cls.v1 = 'value1'
        cls.v2 = 'value2'

    def setUp(self):
        self.r.execute_command('zclear', self.k1)
        self.r.execute_command('zclear', self.k2)
        pass

    def random_string(n):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))

    def test_zadd(self):
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertEqual(self.r.zcard(self.k1), 200)
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 0)
        self.assertEqual(self.r.zcard(self.k1), 200)
        # test for add multiple member score
        self.assertEqual(self.r.zadd(self.k1, 1, str(1), 200, str(200)), 1)
        self.assertEqual(self.r.zcard(self.k1), 201)

    def test_zcard(self):
        self.assertEqual(self.r.zcard(self.k1), 0)
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertEqual(self.r.zcard(self.k1), 200)
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 0)
        self.assertEqual(self.r.zcard(self.k1), 200)

    def test_zrange(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertListEqual(self.r.zrange(self.k1, 0, -1, False, False), [str(i) for i in range(100)])
        self.assertListEqual(self.r.zrange(self.k1, 10, 20, False, False), [str(i) for i in range(10, 21)])
        self.assertListEqual(self.r.zrange(self.k1, 20, 10, False, False), [])
        # range with scores
        self.assertListEqual(self.r.zrange(self.k1, 10, 20, False, True), [(str(i), i) for i in range(10,21)])

    def test_zrevrange(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertListEqual(self.r.zrevrange(self.k1, 0, -1, False), [str(i) for i in range(99, -1, -1)])
        self.assertListEqual(self.r.zrevrange(self.k1, 10, 20,False), [str(i) for i in range(89, 78, -1)])
        self.assertListEqual(self.r.zrevrange(self.k1, 20, 10,False), [])
        # range with scores
        self.assertListEqual(self.r.zrevrange(self.k1, 10, 20, True), [(str(i), i) for i in range(89, 78, -1)])

    def test_zrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertListEqual(self.r.zrangebyscore(self.k1, '-inf', '+inf'), [str(i) for i in range(100)])
        self.assertListEqual(self.r.zrangebyscore(self.k1, 20, 30, 2, 5), ['22', '23', '24', '25', '26'])
        self.assertListEqual(self.r.zrangebyscore(self.k1, 30, 20), [])
        self.assertListEqual(self.r.zrangebyscore(self.k1, 20, 30, None, None, True), [(str(i), i) for i in range(20, 31)])

    def test_zrevrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertListEqual(self.r.zrevrangebyscore(self.k1, '+inf', '-inf'), [str(i) for i in range(99, -1, -1)])
        self.assertListEqual(self.r.zrevrangebyscore(self.k1, 30, 20, 2, 5), ['28', '27', '26', '25', '24'])
        self.assertListEqual(self.r.zrevrangebyscore(self.k1, 20, 30), [])
        self.assertListEqual(self.r.zrevrangebyscore(self.k1, 30, 20, None, None, True), [(str(i), i) for i in range(30, 19, -1)])

    def test_zremrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertEqual(self.r.zremrangebyscore(self.k1, 21, 30), 10)

    def test_zrangebylex(self):
        self.assertEqual(self.r.zadd(self.k1, 1, 'aaa', 2, 'aab', 3, 'abc', 4, 'bcd', 5, 'fff'), 5)
        self.assertListEqual(self.r.zrangebylex(self.k1, '(aaa', '[ccc'), ['aab', 'abc', 'bcd'])

    def test_zrevrangebylex(self):
        self.assertEqual(self.r.zadd(self.k1, 1, 'aaa', 2, 'aab', 3, 'abc', 4, 'bcd', 5, 'fff'), 5)
        self.assertListEqual(self.r.zrevrangebylex(self.k1, '[ccc', '(aaa'), ['bcd', 'abc', 'aab'])

    def test_zremrangebylex(self):
        self.assertEqual(self.r.zadd(self.k1, 1, 'aaa', 2, 'aab', 3, 'abc', 4, 'bcd', 5, 'fff'), 5)
        self.assertEqual(self.r.zremrangebylex(self.k1, '(aaa', '[ccc'), 3)

    def test_zcount(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        self.assertEqual(self.r.zcount(self.k1, 50, 100), 50)

    def test_zlexcount(self):
        self.assertEqual(self.r.zadd(self.k1, 1, 'aaa', 2, 'aab', 3, 'abc', 4, 'bcd', 5, 'fff'), 5)
        self.assertEqual(self.r.zlexcount(self.k1, '(aaa', '[ccc'), 3)

    def test_zscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        for i in range(100):
            self.assertEqual(self.r.zscore(self.k1, str(i)), i)

    def test_zrem(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, i, str(i)), 1)
        for i in range(10, 100):
            self.assertEqual(self.r.zrem(self.k1, str(i)), 1)
        self.assertEqual(self.r.zcard(self.k1), 10)

    def test_zincrby(self):
        self.assertEqual(self.r.zadd(self.k1, 10, 'member1'), 1)
        self.assertEqual(self.r.zincrby(self.k1, 'member1', 100), 110)
        self.assertEqual(self.r.zscore(self.k1, 'member1'), 110)

    def test_zpexpire(self):
        self.assertEqual(self.r.zadd(self.k1, 10, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('zpexpire', self.k1, 5000))
        self.assertLessEqual(self.r.execute_command('zpttl', self.k1), 5000)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def test_zpexpireat(self):
        self.assertEqual(self.r.zadd(self.k1, 10, self.v1), 1)
        # expire in 5s
        ts = int(round(time.time()*1000)) + 5000
        self.assertTrue(self.r.execute_command('zpexpireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('zpttl', self.k1), 5000)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def test_zexpire(self):
        self.assertEqual(self.r.zadd(self.k1, 10, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('zexpire', self.k1, 5))
        self.assertLessEqual(self.r.execute_command('zttl', self.k1), 5)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def test_zexpireat(self):
        self.assertEqual(self.r.zadd(self.k1, 10, self.v1), 1)
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertTrue(self.r.execute_command('zexpireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('zttl', self.k1), 5)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('zclear', cls.k1)
        cls.r.execute_command('zclear', cls.k2)
        print '\nclean up\n'
