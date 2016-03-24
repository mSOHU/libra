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

from libra.services.zmq_socket import ZmqSocketWatcher

LOGGER = logging.getLogger(__name__)


class _Listener(object):
    """listens stateless event from zmq, the delivery is NOT guaranteed.
    """
    def __init__(self):
        # zmq
        self.context = zmq.Context()
        subscriber = self.subscriber = self.context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.RCVTIMEO, 30000)

        self.watcher = ZmqSocketWatcher(
            service_name='zmq',
            strategy='choice',
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
            else:
                for callback, json_decode in self.callbacks[routing_key]:
                    try:
                        headers = json.loads(headers)

                        if json_decode:
                            contents = json.loads(contents)
                    except (TypeError, ValueError) as err:
                        LOGGER.exception(
                            'Exception %r while decoding message: %r, %r, %s',
                            err, routing_key, headers, contents
                        )
                    else:
                        try:
                            callback(routing_key=routing_key, headers=headers, payload=contents)
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
