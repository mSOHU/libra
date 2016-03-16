#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/16/2016 2:49 PM
"""

import time

from libra.services.zmq_broker import ZmqPublisher
from libra.utils import init_logging


init_logging(standalone=True)
instance = ZmqPublisher.get_instance()

while True:
    instance.publish('A', 'hi there')
    time.sleep(1)

