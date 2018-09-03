#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 yongman <yming0221@gmail.com>
#
# Distributed under terms of the MIT license.

"""
unit test for string type
"""

import unittest
import time
from rediswrap import RedisWrapper

class StringTest(unittest.TestCase):
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

    def test_get(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        v1 = self.r.get(self.k1)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))

    def test_set(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        v1 = self.r.get(self.k1)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))

    def test_set_expire(self):
        self.assertTrue(self.r.set(self.k2, self.v2, px=5000))
        v2 = self.r.get(self.k2)
        self.assertEqual(self.v2, v2, '{} != {}'.format(v2, self.v2)) 

        self.assertTrue(self.r.set(self.k2, self.v1, ex=5))
        v1 = self.r.get(self.k2)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))

    def test_set_exists(self):
        self.assertTrue(self.r.set(self.k2, self.v2, nx=True))
        v2 = self.r.get(self.k2)
        self.assertEqual(self.v2, v2, '{} != {}'.format(v2, self.v2))
    
        self.assertTrue(self.r.set(self.k2, self.v1, xx=True))
        v1 = self.r.get(self.k2)
        self.assertEqual(self.v1, v1, '{} != {}'.format(self.v1, v1))

        self.assertTrue(self.r.set(self.k2, self.v2, ex=5, xx=True))
        v2 = self.r.get(self.k2)
        self.assertEqual(self.v2, v2, '{} != {}'.format(v2, self.v2))

    def test_setbit(self):
        ret = self.r.setbit(self.k1, self.bitPos, self.bitVal)
        self.assertEqual(ret, 1-self.bitVal, '{} != {}'.format(ret, 1-self.bitVal))

    def test_getbit(self):
        ret = self.r.setbit(self.k1, self.bitPos, self.bitVal)
        self.assertEqual(ret, 1-self.bitVal, '{} != {}'.format(ret, 1-self.bitVal))
        ret = self.r.getbit(self.k1, self.bitPos)
        self.assertEqual(ret, self.bitVal, '{} != {}'.format(ret, self.bitVal))

    def test_bitcount(self):
        self.r.set(self.k1, 'foobar')
        ret = self.r.bitcount(self.k1)
        self.assertEqual(ret, 26, '{} != {}'.format(ret, '2'))
    
    def test_del(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        v1 = self.r.get(self.k1)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))
        v1 = self.r.delete(self.k1)
        self.assertEqual(v1, 1, '{} != 1'.format(v1))
        v1 = self.r.get(self.k1)
        self.assertIsNone(v1, '{} != None'.format(v1))

    def test_mget(self):
        self.assertTrue(self.r.mset({self.k1:self.v1, self.k2:self.v2}))
        self.assertListEqual(self.r.mget(self.k1, self.k2), [self.v1, self.v2])

    def test_mset(self):
        self.assertTrue(self.r.mset({self.k1:self.v1, self.k2:self.v2}))
        self.assertListEqual(self.r.mget(self.k1, self.k2), [self.v1, self.v2])

    def test_incr(self):
        # incr a new key
        self.assertEqual(self.r.incr(self.k1), 1)
        # incr a valid number key
        self.assertEqual(self.r.incr(self.k1), 2)

        # incr a invalid number
        self.assertTrue(self.r.set(self.k2, self.v2))

        with self.assertRaises(Exception) as cm:
            self.r.incr(self.k2)
        err = cm.exception
        self.assertEqual(str(err), 'value is not an integer or out of range')

    def test_incrby(self):
        self.assertTrue(self.r.set(self.k1, 12345678))
        self.assertEqual(self.r.incrby(self.k1, 12345678), 24691356)

    def test_decr(self):
        # decr a new key
        self.assertEqual(self.r.decr(self.k1), -1)
        # decr a valid number key
        self.assertEqual(self.r.decr(self.k1), -2)

        # decr a invalid number
        self.assertTrue(self.r.set(self.k2, self.v2))

        with self.assertRaises(Exception) as cm:
            self.r.decr(self.k2)
        err = cm.exception
        self.assertEqual(str(err), 'value is not an integer or out of range')

    def test_decrby(self):
        self.assertTrue(self.r.set(self.k1, 12345678))
        self.assertEqual(self.r.decr(self.k1, 12345679), -1)

    def test_strlen(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        self.assertEqual(self.r.strlen(self.k1), len(self.v1))

    def test_pexpire(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.r.pexpire(self.k1, 5000))
        self.assertLessEqual(self.r.pttl(self.k1), 5000)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def test_pexpireat(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        ts = int(round(time.time()*1000)) + 5000
        self.assertTrue(self.r.pexpireat(self.k1, ts))
        self.assertLessEqual(self.r.pttl(self.k1), 5000)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def test_expire(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.r.expire(self.k1, 5))
        self.assertLessEqual(self.r.ttl(self.k1), 5)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def test_expireat(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertTrue(self.r.expireat(self.k1, ts))
        self.assertLessEqual(self.r.ttl(self.k1), 5)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.delete(cls.k1)
        cls.r.delete(cls.k2)
        print '\nclean up\n'
