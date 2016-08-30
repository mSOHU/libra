#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/24/2016 11:12 AM
"""

import logging

import zmq

from libra.utils import rr_choice, EtcdProfile
from libra.endpoint import EndpointWatcher


LOGGER = logging.getLogger(__name__)


class ZmqSocketWatcher(object):
    """etcd-controlled-endpoints dynamic socket
    """
    def __init__(self, service_name, profile, strategy, socket):
        """
        :type profile: EtcdProfile
        """
        self.service_name = service_name
        self.profile = profile
        self.strategy = strategy
        self.socket = socket
        self.endpoint = None
        self.endpoint_list = []

        self.watcher = EndpointWatcher(
            service_name=self.service_name,
            profile=self.profile,
            strategy=strategy,
            switch_callback=self.switch_endpoint
        )

    def switch_endpoint(self, old_endpoint_list, endpoint_list, old_endpoint=None, **_):
        if self.strategy == 'choice':
            self.endpoint = rr_choice(endpoint_list)

            if old_endpoint:
                try:
                    self.socket.disconnect(old_endpoint)
                except zmq.ZMQError:
                    pass

            self.socket.connect(self.endpoint)
            return self.endpoint
        elif self.strategy == 'all':
            for old_endpoint in old_endpoint_list:
                if old_endpoint not in endpoint_list:
                    try:
                        self.socket.disconnect(old_endpoint)
                    except zmq.ZMQError:
                        pass

            self.endpoint_list = endpoint_list
            for endpoint in self.endpoint_list:
                if endpoint not in old_endpoint_list:
                    self.socket.connect(endpoint)
