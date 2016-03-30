# coding: utf-8

import time
import random
import logging
from collections import deque

logger = logging.getLogger(__name__)


class BaseManager(object):
    def get_node(self):
        raise NotImplementedError()

    def release_node(self, node, time_cost=0):
        raise NotImplementedError()

    def dead_node(self, node, time_cost=0):
        raise NotImplementedError()

    def get_node_counter(self):
        raise NotImplementedError()


class WeightNodes(BaseManager):
    """按权重返回node
    """
    COST_HISTORY_COUNT = 100
    MAX_STEP = 2 ** 32

    def __init__(self, weight_table, recovery_num=1000):
        """
        Args:
            weight_table: 字典类型，权重对应表
        """
        self._step = 0
        self._weight_node = {}
        self._live_nodes = []
        self._live = set()
        self._fail = set()
        self._recovery_num = recovery_num
        self._node_counter = {}

        for k, v in weight_table.iteritems():
            self._weight_node[k] = v
            self._live.add(k)
            self._live_nodes.extend([k] * v)
            self._node_counter[k] = {
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

    def get_node(self):
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

    def release_node(self, node, time_cost=0):
        if node in self._fail:
            self._fail.remove(node)
            self._live.add(node)
            v = self._weight_node[node]
            self._live_nodes.extend([node] * v)
            self._live_len = len(self._live_nodes)
            random.shuffle(self._live_nodes)
            logger.info(u'恢复node:%s' % node)

        node_counter = self._node_counter[node]
        node_counter['release'] += 1
        node_counter['time_cost'] += time_cost
        node_counter['time_cost_history'].append(time_cost)
        node_counter['state'] = 'ok'

    def dead_node(self, node, time_cost=0):
        if node in self._live:
            self._fail.add(node)
            self._live.remove(node)
            self._live_nodes[:] = filter(lambda x: x != node, self._live_nodes)
            self._live_len = len(self._live_nodes)
            random.shuffle(self._live_nodes)

        node_counter = self._node_counter[node]
        node_counter['dead'] += 1
        node_counter['time_cost'] += time_cost
        node_counter['time_cost_history'].append(time_cost)
        node_counter['state'] = 'fail'
        node_counter['last_fail'] = time.time()

        logger.info(u'剔除node:%s' % node)

        if not self._live_nodes:
            logger.error(u"所有节点都失败了，请立刻检测")

    def get_node_counter(self):
        return self._node_counter
