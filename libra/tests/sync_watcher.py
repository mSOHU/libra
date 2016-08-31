#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 2/26/2016 3:19 PM
"""


import time
import random

from libra.utils import init_logging, get_etcd, EtcdProfile
from libra.watcher import Watcher


PROFILE = EtcdProfile.DEVELOP


def watch_fragments(action, key, value, **kwargs):
    print action, key, value, kwargs.get('prev_value'), kwargs
    time.sleep(2)

watcher = Watcher(
    '/fragments',
    profile=PROFILE,
    change_callback=watch_fragments,
    final_state=True,
    # sync_mode=True,
)


if __name__ == '__main__':
    init_logging(standalone=True)
    etcd = get_etcd(profile=PROFILE)
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
