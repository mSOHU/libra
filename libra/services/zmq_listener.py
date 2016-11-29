#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/15/2016 4:35 PM
"""

import json
import time
import logging
import threading
from collections import defaultdict

import zmq

from libra.utils import EtcdProfile
from libra.services.zmq_socket import ZmqSocketWatcher, SwitchStrategy

LOGGER = logging.getLogger(__name__)


class ZmqListener(object):
    """listens stateless event from zmq, the delivery is NOT guaranteed.
    """
    SERVICE_NAME = 'zmq:broker'

    def __init__(self, profile):
        """
        :type profile: EtcdProfile
        """
        # zmq
        self.context = zmq.Context()
        subscriber = self.subscriber = self.context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.RCVTIMEO, 30000)

        self.profile = profile
        self.watcher = ZmqSocketWatcher(
            service_name=self.SERVICE_NAME,
            profile=self.profile,
            strategy=SwitchStrategy.CHOSEN,
            socket=self.subscriber
        )

        self.callbacks = defaultdict(list)
        self.prefixes = []

        # listener thread
        self.listen_thread = threading.Thread(target=self._listen_fn)
        self.listen_thread.daemon = True
        self.listen_thread.start()

    def _listen_fn(self):
        while True:
            try:
                routing_key, headers, contents = self.subscriber.recv_multipart()
            except zmq.Again:
                continue
            except Exception as err:
                LOGGER.exception('%r, while listening events.', err)
                # avoid potential dead loop
                time.sleep(1)
                continue

            try:
                headers = json.loads(headers)
                contents = json.loads(contents)
            except (TypeError, ValueError) as err:
                LOGGER.exception(
                    'Exception %r while decoding message: %r, %r, %s',
                    err, routing_key, headers, contents
                )
                # avoid potential dead loop
                time.sleep(1)
                continue

            for callback in self.callbacks[routing_key]:
                try:
                    callback(routing_key=routing_key, headers=headers, payload=contents)
                except Exception as err:
                    LOGGER.exception(
                        'Exception %r while invoking callback %s:%r',
                        err, routing_key, callback
                    )

    def listen(self, routing_keys):
        # only str is acceptable
        if isinstance(routing_keys, str):
            routing_keys = [routing_keys]

        assert routing_keys, 'no routing_key provided'
        routing_keys = routing_keys or []

        def decorator(fn):
            for routing_key in routing_keys:
                if routing_key not in self.callbacks:
                    self.subscriber.setsockopt(zmq.SUBSCRIBE, routing_key)

                self.callbacks[routing_key].append(fn)

        return decorator
