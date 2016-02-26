#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 2/26/2016 3:19 PM
"""


import time
import random

from libra.utils import init_logging, get_etcd
from libra.watcher import watch


@watch('/fragments')
def watch_fragments(action, key, value):
    print action, key, value


if __name__ == '__main__':
    init_logging(standalone=True)
    etcd = get_etcd()
    while True:
        time.sleep(2)
        print time.ctime()
        etcd.set('/fragments/helpme', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
