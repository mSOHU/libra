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
        self.prefixes = prefixes or []
        self.json_decode = json_decode
        self.watcher = Watcher(
            self.SERVICE_PATH,
            change_callback=self.on_service_change,
            init_callback=self.on_service_init,
        )

        # zmq
        self.zmq_context = None
        self.zmq_subscriber = None
        self.zmq_endpoints = None
        self.zmq_endpoint = None

        # listener thread
        self.listen_thread = threading.Thread(target=self._listen_fn)
        self.listen_thread.daemon = True
        self.listen_thread.start()

    def on_service_change(self, key, value, **kwargs):
        if key == 'endpoints':
            self.zmq_endpoints = json.loads(value)['endpoints']
            # if current endpoint not in new endpoint list, then we requires rebuild the context
            if self.zmq_endpoint not in self.zmq_endpoints:
                endpoint = rr_choice(self.zmq_endpoints)
                self.rebuild_context(endpoint)
                self.zmq_endpoint = endpoint
                LOGGER.info('Listening at endpoint: %s, prefix: %r' % (self.zmq_endpoint, self.prefixes))

    def on_service_init(self, root):
        for node in root.leaves:
            self.on_service_change(node.key[len(self.SERVICE_PATH)+1:], node.value)

    def rebuild_context(self, endpoint):
        if self.zmq_context is not None:
            self.zmq_subscriber.close()
            self.zmq_context.term()
            self.zmq_context = self.zmq_subscriber = None

        context = self.zmq_context = zmq.Context()
        subscriber = context.socket(zmq.SUB)
        subscriber.connect(endpoint)
        if self.prefixes:
            for prefix in self.prefixes:
                subscriber.setsockopt(zmq.SUBSCRIBE, prefix)
        subscriber.setsockopt(zmq.RCVTIMEO, 30000)
        self.zmq_subscriber = subscriber

    def _listen_fn(self):
        while True:
            if not self.zmq_subscriber:
                time.sleep(.2)
                continue

            try:
                address, contents = self.zmq_subscriber.recv_multipart()
            except zmq.Again:
                continue
            except Exception as err:
                LOGGER.exception('%r, while listening events.', err)
                # avoid potential dead loop
                time.sleep(1)
            else:
                try:
                    if self.json_decode:
                        contents = json.loads(self.json_decode)

                    self.callback(contents)
                except Exception as err:
                    LOGGER.exception('Exception %r while invoking callback %r', err, self.callback)
