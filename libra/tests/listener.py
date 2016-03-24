#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/15/2016 5:02 PM
"""

from libra.listener import listen
from libra.utils import init_logging


init_logging(standalone=True)


@listen('A')
def listener(routing_key, payload, headers, **_):
    print '[+] event received: %s, %s: %s' % (routing_key, headers, payload)


raw_input('Waiting for events...')
