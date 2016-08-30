#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/16/2016 2:49 PM
"""

import time

from libra.services.zmq_broker import ZmqBroker
from libra.utils import init_logging


init_logging(standalone=True)
instance = ZmqBroker.get_instance(profile='develop')

while True:
    instance.publish('A', 'hi there', headers={'src': 'oh!'})
    time.sleep(1)

