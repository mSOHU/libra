#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/15/2016 5:02 PM
"""

from libra.listener import listen
from libra.utils import init_logging


init_logging(standalone=True)


@listen('template')
def listener(routing_key, payload, **_):
    print '[+] event received: %s:%s' % (routing_key, payload)


raw_input('Waiting for events...')
