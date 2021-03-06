#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/25/2016 12:44 PM
"""

import time
import random
import logging
import threading
from collections import deque, defaultdict

from libra.weight import calc_weight
from libra.manager import BaseManager
from libra.endpoint import EndpointWatcher, SwitchStrategy
from libra.utils import local_ip, extract_netloc, EtcdProfile


logger = logging.getLogger(__name__)


class WeightEndpoints(BaseManager):
    """按权重返回node
    """
    COST_HISTORY_COUNT = 100
    MAX_STEP = 2 ** 32

    def __init__(
            self, service_name, profile, recovery_num=1000, **weight_kwargs):
        """
            :param weight_table: 字典类型，权重对应表
            :type profile: EtcdProfile
        """
        self._step = 0
        self._weight_node = {}
        self._live_nodes = []
        self._live_len = 0
        self._live = set()
        self._fail = set()
        self._recovery_num = recovery_num
        self._node_counter = defaultdict(dict)
        self._ready = threading.Event()

        # dynamic service
        self.service_name = service_name
        self.profile = profile
        self.weight_kwargs = weight_kwargs
        self.watcher = EndpointWatcher(
            service_name=self.service_name,
            profile=self.profile,
            strategy=SwitchStrategy.ANY,
            switch_callback=self._switch_endpoint,
        )

    def _switch_endpoint(self, endpoint_list, old_endpoint_list, **_):
        try:
            weight_table = calc_weight(
                local_ip=local_ip(),
                remote_nodes={
                    extract_netloc(endpoint, without_port=True): endpoint
                    for endpoint in endpoint_list
                    if endpoint not in old_endpoint_list
                },
                **self.weight_kwargs
            )
        except Exception as err:
            logger.exception('Unable to calculate weight table, %r', err)
            return

        self._ready.clear()

        for endpoint in old_endpoint_list:
            self._node_counter[endpoint]['state'] = 'removed'
            if endpoint not in endpoint_list:
                try:
                    self._fail.remove(endpoint)
                except KeyError:
                    pass
                try:
                    self._live.remove(endpoint)
                except KeyError:
                    pass
                self._live_nodes[:] = filter(lambda x: x != endpoint, self._live_nodes)
                self._live_len = len(self._live_nodes)

        for endpoint, weight in weight_table.iteritems():
            self._weight_node[endpoint] = weight
            self._live.add(endpoint)
            self._live_nodes.extend([endpoint] * weight)
            if endpoint not in self._node_counter:
                self._node_counter[endpoint] = {
                    'get': 0,
                    'release': 0,
                    'dead': 0,
                    'state': 'ok',
                    'time_cost': 0,
                    'time_cost_history': deque(maxlen=self.COST_HISTORY_COUNT),
                    'last_fail': 'never',
                }

        self._live_len = len(self._live_nodes)
        random.shuffle(self._live_nodes)
        self._ready.set()

    def _wait_ready(self, timeout=None):
        self._ready.wait(timeout)

    def get_node(self):
        self._wait_ready()
        self._step += 1
        self._step ^= self.MAX_STEP

        # 重试机制
        if self._fail and self._step % self._recovery_num == 0:
            node = random.choice(list(self._fail))
            self._node_counter[node]['get'] += 1
            return node

        # 节点全失效时的处理
        if not self._live_nodes:
            node = random.choice(list(self._fail))  # 如果self._fail为空会失败哦，应该不会失败
            self._node_counter[node]['get'] += 1
            return node

        step = self._step % self._live_len
        node = self._live_nodes[step]
        self._node_counter[node]['get'] += 1
        return node

    def release_node(self, node, time_cost=None):
        self._wait_ready()
        if node in self._fail:
            self._fail.remove(node)
            self._live.add(node)
            v = self._weight_node[node]
            self._live_nodes.extend([node] * v)
            self._live_len = len(self._live_nodes)
            random.shuffle(self._live_nodes)
            logger.info('Endpoint [%s / %s] recovered.', self.service_name, node)

        node_counter = self._node_counter[node]
        node_counter['release'] += 1
        if time_cost is not None:
            node_counter['time_cost'] += time_cost
            node_counter['time_cost_history'].append(time_cost)
        node_counter['state'] = 'ok'

    def dead_node(self, node, time_cost=None):
        if node in self._live:
            self._fail.add(node)
            self._live.remove(node)
            self._live_nodes[:] = filter(lambda x: x != node, self._live_nodes)
            self._live_len = len(self._live_nodes)
            random.shuffle(self._live_nodes)

        node_counter = self._node_counter[node]
        node_counter['dead'] += 1
        if time_cost is not None:
            node_counter['time_cost'] += time_cost
            node_counter['time_cost_history'].append(time_cost)
        node_counter['state'] = 'fail'
        node_counter['last_fail'] = time.time()

        logger.info('Endpoint [%s / %s] marked as down.', self.service_name, node)

        if not self._live_nodes:
            logger.error(
                'All endpoints failed! Service %s', self.service_name)

    def get_node_counter(self):
        return self._node_counter


