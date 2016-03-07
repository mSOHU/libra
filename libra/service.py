#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 1/18/2016 6:20 PM
"""

import logging
import functools
from collections import defaultdict

from libra.watcher import Watcher


LOGGER = logging.getLogger(__name__)


class ServiceUnavailable(Exception):
    def __init__(self, fn, services):
        self.fn = fn
        self.services = services


class ServiceManager(object):
    """
    @manager.depends('redis.main_read')
    def test():
        return main_redis.lrange('some_key', 0, -1)

    @test.downgrade
    def test_down():
        return []

    -- or --

    @manager.downgrade(test)
    def test_down():
        return []
    """
    SERVICES_PATH = '/static-services'

    def __init__(self, prefix=None):
        self.prefix = prefix
        self.statuses = defaultdict(lambda: 'unknown')
        self.watcher = Watcher(
            self.SERVICES_PATH,
            change_callback=self.on_change,
            init_callback=self.init_statuses,
            prefix=prefix
        )

    def depends(self, *services):
        def decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                for s in services:
                    if self.statuses[s] != 'ready':
                        break
                else:
                    return fn(*args, **kwargs)

                return downgrade_fn[0](*args, **kwargs)

            def default_downgrade(*_, **__):
                raise ServiceUnavailable(fn, services)

            downgrade_fn = [default_downgrade]

            def _downgrade(dfn):
                downgrade_fn[0] = dfn
                return dfn

            wrapper.downgrade = _downgrade
            return wrapper
        return decorator

    @classmethod
    def downgrade(cls, func):
        if isinstance(func, (classmethod, staticmethod)):
            func = func.__func__

        # handle gen.engine
        if getattr(func, 'func_closure', None):
            func_code = func.func_code
            if func_code.co_filename.endswith('gen.py') and func_code.co_name == 'wrapper':
                func = func.func_closure[0].cell_contents

        # handle lru_cached_function
        if type(func).__name__ == 'LRUCachedFunction' and hasattr(func, 'function'):
            func = func.function

        return func.downgrade

    def init_statuses(self, root):
        for item in root.leaves:
            item_key = item.key
            if item.dir or not item_key.endswith('/status'):
                continue

            assert item_key.startswith(self.watcher.watch_path)
            service_name = item_key[len(self.watcher.watch_path)+1:-len('/status')].replace('/', '.')
            self.update_status(service_name, item.value)

    def update_status(self, service_name, new_value):
        old_value = self.statuses[service_name]
        log_fn = LOGGER.info if old_value != new_value else LOGGER.debug
        log_fn('service status changed: `%s` [%s] -> [%s]', service_name, old_value, new_value)
        self.statuses[service_name] = new_value

    def set_status(self, service_name, new_value):
        """set service status to server"""
        self.watcher.server.set('%s/%s/%s/status' % (
            self.SERVICES_PATH, self.prefix,
            service_name.replace('.', '/')), new_value)

    def get_statuses(self):
        return dict(self.statuses)

    def on_change(self, action, key, value, is_dir, **_):
        """
        action -> set
          - update status
          - add server

        action -> delete
          - remove server(s)

        """
        item_key = key
        if action == 'set':
            if item_key.endswith('/status'):
                service_name = item_key[1:-len('/status')].replace('/', '.')
                self.update_status(service_name, value)
        elif action == 'delete':
            if item_key.endswith('/status'):
                service_base = item_key[1:-len('/status')].replace('/', '.')
            elif is_dir:
                service_base = item_key[1:].replace('/', '.')
            else:
                return

            for service_name in self.statuses:
                if service_name.startswith(service_base):
                    self.update_status(service_name, 'unknown')
