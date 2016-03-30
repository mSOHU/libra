#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 2/26/2016 3:25 PM
"""

import time
import logging
import threading

import etcd

from libra.utils import get_etcd


LOGGER = logging.getLogger(__name__)


class Watcher(object):
    def __init__(self, path, change_callback, init_callback=None, prefix=None):
        self.prefix = prefix
        self.change_callback = change_callback
        self.init_callback = init_callback
        self.server = get_etcd()
        self.watch_path = '%s/%s' % (path, prefix) if prefix else path
        self.watcher_thread = threading.Thread(target=self._watcher_fn)
        self.watcher_thread.daemon = True
        self.watcher_thread.start()

    def _watcher_fn(self):
        initial_item = self.server.read(self.watch_path, recursive=True)
        if callable(self.init_callback):
            # we don't process exceptions, because this means coding issue
            try:
                self.init_callback(initial_item)
            except Exception as err:
                LOGGER.exception('%r, while invoking init_callback', err)
                return

        current_index = self.calc_max_index(initial_item) + 1

        while True:
            try:
                # we don't use timeout=0 to prevent fake death of connection,
                # due our network situation is not reliable
                item = self.server.watch(self.watch_path, current_index, timeout=60, recursive=True)
            except etcd.EtcdWatchTimedOut:
                continue
            except etcd.EtcdEventIndexCleared as err:
                new_index = err.payload['index']
                LOGGER.info('Etcd: %s [%u -> %u]', err.payload['cause'], current_index, new_index)
                root = self.server.read(self.watch_path, recursive=True)
                self.resync_statuses(root, current_index)
                current_index = new_index
                continue
            except Exception as err:
                LOGGER.exception('%r, while watching service status', err)
                # avoid potential dead loop
                time.sleep(2)
                continue
            else:
                self.on_change(item)
                current_index = item.modifiedIndex + 1

    def resync_statuses(self, root, current):
        for item in root.leaves:
            if current >= item.modifiedIndex:
                continue

            self.on_change(item)

    def on_change(self, item):
        item_key = item.key
        assert item_key.startswith(self.watch_path)

        item_key = item_key[len(self.watch_path):]
        try:
            self.change_callback(action=item.action, key=item_key, value=item.value, is_dir=item.dir)
        except Exception as err:
            LOGGER.exception('Exception %r while invoking callback %r', err, self.change_callback)

    @classmethod
    def calc_max_index(cls, root):
        max_index = root.modifiedIndex
        for item in root.leaves:
            max_index = max(max_index, item.modifiedIndex)

        return max_index

    @classmethod
    def watch(cls, path, prefix=None):
        def decorator(fn):
            cls(path, fn, prefix)
            return fn

        return decorator


watch = Watcher.watch
