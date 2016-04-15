#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 2/26/2016 3:19 PM
"""


import time
import random

from libra.utils import init_logging, get_etcd
from libra.watcher import Watcher


def watch_fragments(action, key, value, **_):
    print action, key, value, _.get('prev_value'), _
    time.sleep(2)

watcher = Watcher(
    '/fragments',
    change_callback=watch_fragments,
    final_state=True,
    # sync_mode=True,
)


if __name__ == '__main__':
    init_logging(standalone=True)
    etcd = get_etcd()
    watcher.loop_forever()
    while True:
        time.sleep(2)
        print time.ctime()
        etcd.set('/fragments/helpme', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
        etcd.set('/fragments/helpme1', random.choice(range(10)))
