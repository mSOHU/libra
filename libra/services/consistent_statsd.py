#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 8/19/2016 12:55 PM
"""

import logging
import threading

import statsd
import hash_ring
import uritools

from libra.utils import EtcdProfile
from libra.endpoint import EndpointWatcher, SwitchStrategy


logger = logging.getLogger(__name__)


class ConsistentStatsdClient(object):
    STATSD_PROTOCOLS = {
        'statsd+udp': statsd.StatsClient,
        'statsd+tcp': statsd.TCPStatsClient,
    }

    def __init__(self, service_name, profile):
        """
            :type profile: EtcdProfile
        """
        self.profile = profile
        self.endpoint_ring = None
        self.clients = {}
        self._ready = threading.Event()

        # dynamic service
        self.service_name = service_name
        self.watcher = EndpointWatcher(
            service_name=self.service_name,
            profile=self.profile,
            strategy=SwitchStrategy.ANY,
            switch_callback=self._switch_endpoint,
        )

    @classmethod
    def build_client(cls, url):
        to_bool = lambda s: s in ('true', 'True', '1')
        type_conversion = {
            'maxudpsize': int,  # udp only
            'timeout': float,  # tcp only
            'prefix': str,
            'ipv6': to_bool,
        }

        parts = uritools.urisplit(url)
        conn_kwargs = dict(host=parts.host, port=int(parts.port))
        if parts.query:
            query_args = parts.getquerylist()
            for key, value in query_args:
                if key in type_conversion:
                    fn = type_conversion[key]
                    value = fn(value)

                conn_kwargs[key] = value

        return cls.STATSD_PROTOCOLS[parts.scheme](**conn_kwargs)

    def _switch_endpoint(self, endpoint_list, **_):
        self.endpoint_ring = hash_ring.HashRing(nodes=endpoint_list)
        self.clients = {
            endpoint_url: self.build_client(endpoint_url)
            for endpoint_url in endpoint_list
        }
        self._ready.set()

    def _wait_ready(self, timeout=None):
        self._ready.wait(timeout)

    def get_node(self, key):
        self._wait_ready()
        return self.clients[self.endpoint_ring.get_node(str(key))]
