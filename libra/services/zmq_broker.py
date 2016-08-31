#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/16/2016 2:39 PM
"""

import json
import logging

import zmq

from libra.watcher import Watcher
from libra.utils import utf8, local_ip


LOGGER = logging.getLogger(__name__)


class ZmqBroker(object):
    LEADER_PATH = '/services/zmq:broker/leader'
    INSTANCES = {}

    def __init__(self, profile):
        self.profile = profile
        self.watcher = Watcher(
            self.LEADER_PATH,
            profile=self.profile,
            change_callback=self._on_leader_change,
            init_callback=self._on_leader_init,
        )
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.publisher = None
        self.sender_ip = local_ip()

    def _on_leader_init(self, root):
        self._on_leader_change(value=root.value)

    def _on_leader_change(self, value, **_):
        publisher = json.loads(value)['publisher']
        if self.publisher == publisher:
            return

        self.publisher, old_publisher = publisher, self.publisher

        try:
            if old_publisher:
                self.socket.disconnect(old_publisher)
        except zmq.ZMQError:
            pass

        self.socket.connect(self.publisher)
        LOGGER.info('Leader pointed to %s.', self.publisher)

    def publish(self, routing_key, message, headers=None):
        if not isinstance(message, basestring):
            message = json.dumps(message)
        else:
            message = utf8(message)

        headers = headers or {}
        assert isinstance(headers, dict), 'Invalid header type: %s' % type(headers)
        headers.setdefault('sender_ip', self.sender_ip)
        self.socket.send_multipart([routing_key, json.dumps(headers), message])

    @classmethod
    def get_instance(cls, profile):
        """
        :rtype: ZmqBroker
        """
        instance = cls.INSTANCES.get(profile)
        if not instance:
            instance = cls.INSTANCES[profile] = cls(profile=profile)

        return instance
