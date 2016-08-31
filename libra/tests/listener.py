#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/15/2016 5:02 PM
"""

from libra.services.zmq_listener import ZmqListener
from libra.utils import init_logging, EtcdProfile


init_logging(standalone=True)

listen = ZmqListener(profile=EtcdProfile.DEVELOP).listen


@listen('A')
def listener(routing_key, payload, headers, **_):
    print '[+] event received: %s, %s: %s' % (routing_key, headers, payload)


raw_input('Waiting for events...')
