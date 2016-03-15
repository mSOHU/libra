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

import zmq

from libra.watcher import Watcher
from libra.utils import rr_choice

LOGGER = logging.getLogger(__name__)


class Listener(object):
    """listens stateless event from zmq, the delivery is NOT guaranteed.
    """
    SERVICE_PATH = '/services/zmq'

    def __init__(self, callback, prefixes=None, json_decode=False):
        self.callback = callback

        # only str is acceptable
        if isinstance(prefixes, str):
            prefixes = [prefixes]

        self.prefixes = prefixes or []
        self.json_decode = json_decode
        self.watcher = Watcher(
            self.SERVICE_PATH,
            change_callback=self.on_service_change,
            init_callback=self.on_service_init,
        )

        # zmq
        self.context = zmq.Context()
        subscriber = self.subscriber = self.context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.RCVTIMEO, 30000)

        if self.prefixes:
            for prefix in self.prefixes:
                subscriber.setsockopt(zmq.SUBSCRIBE, prefix)
        else:
            subscriber.setsockopt(zmq.SUBSCRIBE, '')

        self.endpoint_list = None
        self.endpoint = None

        # listener thread
        self.listen_thread = threading.Thread(target=self._listen_fn)
        self.listen_thread.daemon = True

    def on_service_change(self, key, value, **kwargs):
        if key == 'endpoints':
            self.endpoint_list = json.loads(value)['endpoints']

            # if current endpoint not in new endpoint list, then we requires rebuild the context
            if self.endpoint not in self.endpoint_list:
                self.rebuild_context(rr_choice(self.endpoint_list))
                LOGGER.info('Listening at endpoint: %s, prefix: %r' % (self.endpoint, self.prefixes))

    def on_service_init(self, root):
        for node in root.leaves:
            self.on_service_change(node.key[len(self.SERVICE_PATH)+1:], node.value)

        self.listen_thread.start()

    def rebuild_context(self, endpoint):
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
                try:
                    if self.json_decode:
                        contents = json.loads(contents)

                    self.callback(routing_key=routing_key, payload=contents)
                except Exception as err:
                    LOGGER.exception('Exception %r while invoking callback %r', err, self.callback)

    @classmethod
    def listen(cls, prefixes=None, json_decode=False):
        def decorator(fn):
            cls(fn, prefixes=prefixes, json_decode=json_decode)

        return decorator


listen = Listener.listen
