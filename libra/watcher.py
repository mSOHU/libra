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

from libra.utils import get_etcd, EtcdProfile


LOGGER = logging.getLogger(__name__)


class Watcher(object):
    def __init__(
            self, watch_path, profile, change_callback,
            init_callback=None, sync_mode=False, final_state=False):
        """
        :type profile: EtcdProfile
        :param final_state: indicates ignoring the transition process,
                             just jump to the final state
        """
        self.profile = profile
        self.watch_path = watch_path
        self.change_callback = change_callback
        self.init_callback = init_callback

        self.server = get_etcd(profile=self.profile)

        self.final_state = final_state
        self.sync_mode = sync_mode
        if not self.sync_mode:
            self.watcher_thread = threading.Thread(target=self._watcher_fn)
            self.watcher_thread.daemon = True
            self.watcher_thread.start()

    def read_root(self):
        return self.server.read(self.watch_path, recursive=True)

    def _watcher_fn(self):
        # delay thread start to avoid competition
        time.sleep(0.1)

        initial_item = self.read_root()
        if callable(self.init_callback):
            # we don't process exceptions, because this means coding issue
            try:
                self.init_callback(root=initial_item)
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
                # TODO: if self.final_state:
                self.resync_statuses(current_index)
                current_index = new_index
                continue
            except Exception as err:
                LOGGER.exception('%r, while watching service status', err)
                # avoid potential dead loop
                time.sleep(2)
                continue
            else:
                if self.final_state:
                    root = self.read_root()
                    # passing item is meaningless
                    self.on_change(root, root=root)
                    current_index = self.calc_max_index(root) + 1
                else:
                    self.on_change(item)
                    current_index = item.modifiedIndex + 1

    def resync_statuses(self, current):
        root = self.read_root()
        for item in root.leaves:
            if current >= item.modifiedIndex:
                continue

            self.on_change(item)

    def on_change(self, item, **kwargs):
        item_key = item.key
        assert item_key.startswith(self.watch_path)

        item_key = item_key[len(self.watch_path):]
        kwargs.update({
            'action': item.action,
            'key': item_key,
            'value': item.value,
            'is_dir': item.dir
        })

        if hasattr(item, '_prev_node'):
            kwargs['prev_value'] = item._prev_node.value

        try:
            self.change_callback(**kwargs)
        except Exception as err:
            LOGGER.exception('Exception %r while invoking callback %r', err, self.change_callback)

    def loop_forever(self):
        if self.sync_mode:
            return self._watcher_fn()
        else:
            self.watcher_thread.join()

    @classmethod
    def calc_max_index(cls, root):
        # @see: https://github.com/coreos/etcd/blob/master/Documentation/api.md#watch-from-cleared-event-index
        if hasattr(root, 'etcd_index'):
            return root.etcd_index

        max_index = root.modifiedIndex
        for item in root.leaves:
            max_index = max(max_index, item.modifiedIndex)

        return max_index

    @classmethod
    def watch(cls, path, profile, **kwargs):
        def decorator(fn):
            cls(path, fn, profile=profile, **kwargs)
            return fn

        return decorator


watch = Watcher.watch
