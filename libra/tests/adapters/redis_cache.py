#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 8/31/2016 9:17 PM
"""

from libra.adapters.redis_cache import LibraStrictRedis
from libra.utils import init_logging, EtcdProfile


init_logging(standalone=True)


client = LibraStrictRedis(
    service_name='redis:main',
    profile=EtcdProfile.DEVELOP,
    recovery_num=1000,
    socket_timeout=.05,
    max_retries=3
)

for i in range(100):
    print client.zcard('news')

print client.manager.get_node_counter()

print client
