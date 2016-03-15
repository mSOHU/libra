#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/15/2016 5:02 PM
"""

from libra.listener import Listener
from libra.utils import init_logging


init_logging(standalone=True)


def listener(routing_key, payload, **kwargs):
    print '[+] event received: %s:%s' % (routing_key, payload)


Listener(callback=listener, prefixes=['B'])
raw_input('Waiting for events...')
