#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/24/2016 5:16 PM
"""

import json
import logging

from libra.watcher import Watcher


LOGGER = logging.getLogger(__name__)


class EndpointWatcher(object):
    SERVICE_BASE = '/services/%s/endpoints'

    def __init__(self, service_name, strategy, switch_callback):
        """
        :param strategy: 'choice' or 'all', which means should we care
            choice endpoint change or just the one being chosen
        """
        self.service_name = service_name
        self.service_path = self.SERVICE_BASE % service_name
        self.strategy = strategy
        assert self.strategy in ('choice', 'all'), 'Invalid strategy: %s' % self.strategy
        self.switch_callback = switch_callback

        self.endpoint_list = []
        self.endpoint = None

        self.watcher = Watcher(
            self.SERVICE_BASE % service_name,
            change_callback=self.on_endpoint_change,
            init_callback=self.on_endpoint_init,
        )

    def on_endpoint_change(self, value, **_):
        old_endpoint_list = self.endpoint_list
        self.endpoint_list = json.loads(value)['endpoints']
        assert None not in self.endpoint_list, 'Invalid endpoint value: None'

        if self.strategy == 'choice':
            if self.endpoint not in self.endpoint_list:
                old_endpoint = self.endpoint
                self.endpoint = self.switch_callback(
                    endpoint_list=self.endpoint_list,
                    old_endpoint_list=old_endpoint_list,
                    old_endpoint=old_endpoint
                )
                LOGGER.info(
                    'Service [%s] endpoint switched [%s] -> [%s].',
                    self.service_name, old_endpoint, self.endpoint
                )
        elif self.strategy == 'all':
            if set(self.endpoint_list) != set(old_endpoint_list):
                self.switch_callback(
                    endpoint_list=self.endpoint_list,
                    old_endpoint_list=old_endpoint_list
                )
                LOGGER.info(
                    'Service [%s] endpoint list changed %r -> %r.',
                    self.service_name, old_endpoint_list, self.endpoint_list
                )

    def on_endpoint_init(self, root):
        for node in root.leaves:
            if node.key == self.service_path:
                self.on_endpoint_change(value=node.value)
