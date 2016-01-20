#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 1/19/2016 9:46 PM
"""

import time
import random

from libra.service import ServiceManager

manager = ServiceManager('product')
manager.start_monitor()


@manager.depends('redis.main_read')
def read_function():
    return '[up] main_read is up!'


@read_function.downgrade
def read_down():
    return '[down] main_read is down!'


if __name__ == '__main__':
    while True:
        time.sleep(1)
        print time.ctime(), read_function()
        new_status = 'ready' if random.random() > 0.2 else 'down'
        manager.set_status('redis.main_read', new_status)
