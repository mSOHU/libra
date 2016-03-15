#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/15/2016 5:02 PM
"""

from libra.listener import Listener
from libra.utils import init_logging


init_logging(standalone=True)


def listener(message):
    print '[+] event received: %s' % message


Listener(callback=listener)
raw_input('Waiting for events...')
