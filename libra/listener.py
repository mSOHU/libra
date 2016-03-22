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

from libra.watcher import Watcher
from libra.utils import rr_choice

LOGGER = logging.getLogger(__name__)


class _Listener(object):
    """listens stateless event from zmq, the delivery is NOT guaranteed.
    """
    SERVICE_PATH = '/services/zmq'

    def __init__(self):
        self.watcher = Watcher(
            self.SERVICE_PATH,
            change_callback=self.on_service_change,
            init_callback=self.on_service_init,
        )

        # zmq
        self.context = zmq.Context()
        subscriber = self.subscriber = self.context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.RCVTIMEO, 30000)

        self.callbacks = defaultdict(list)
        self.prefixes = []

        self.endpoint_list = None
        self.endpoint = None

        # listener thread
        self.listen_thread = threading.Thread(target=self._listen_fn)
        self.listen_thread.daemon = True

    def on_service_change(self, key, value, **kwargs):
        if key[1:] == 'endpoints':
            self.endpoint_list = json.loads(value)['endpoints']

            # if current endpoint not in new endpoint list, then we requires rebuild the context
            if self.endpoint not in self.endpoint_list:
                self.switch_endpoint(rr_choice(self.endpoint_list))
                LOGGER.info('Listening at endpoint: %s, prefix: %r' % (
                    self.endpoint,
                    [prefix for prefix, value in self.callbacks.items() if value]
                ))

    def on_service_init(self, root):
        for node in root.leaves:
            self.on_service_change(node.key[len(self.SERVICE_PATH):], node.value)

        self.listen_thread.start()

    def switch_endpoint(self, endpoint):
        self.endpoint, old_endpoint = endpoint, self.endpoint

        try:
            if old_endpoint:
                self.subscriber.disconnect(old_endpoint)
        except zmq.ZMQError:
            pass

        self.subscriber.connect(self.endpoint)

    def _listen_fn(self):
        while True:
            try:
                routing_key, contents = self.subscriber.recv_multipart()
            except zmq.Again:
                continue
            except Exception as err:
                LOGGER.exception('%r, while listening events.', err)
                # avoid potential dead loop
                time.sleep(1)
            else:
                for callback, json_decode in self.callbacks[routing_key]:
                    try:
                        if json_decode:
                            contents = json.loads(contents)

                        callback(routing_key=routing_key, payload=contents)
                    except Exception as err:
                        LOGGER.exception(
                            'Exception %r while invoking callback %s:%r',
                            err, routing_key, callback
                        )

    def listen(self, routing_keys, json_decode=False):
        # only str is acceptable
        if isinstance(routing_keys, str):
            routing_keys = [routing_keys]

        assert routing_keys, 'no routing_key provided'
        routing_keys = routing_keys or []

        def decorator(fn):
            for routing_key in routing_keys:
                if routing_key not in self.callbacks:
                    self.subscriber.setsockopt(zmq.SUBSCRIBE, routing_key)

                self.callbacks[routing_key].append((fn, json_decode))

        return decorator


Listener = _Listener()
listen = Listener.listen
