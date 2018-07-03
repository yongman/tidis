#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 YanMing <***REMOVED***>
#
# Distributed under terms of the MIT license.

"""
redis client wrapper
"""

import redis

class RedisWrapper:
    def __init__(self, ip , port):
        self.r = redis.StrictRedis(host=ip, port=port)

    def get_instance(self):
        return self.r
