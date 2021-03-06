#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 8/31/2016 8:32 PM
"""

import time
import logging

import redis
import redis.client
import uritools

from libra.utils import EtcdProfile, to_bool
from libra.services.weight_endpoint import WeightEndpoints


LOGGER = logging.getLogger(__name__)


def _patch_del(manager, node):
    def __wrap(self):
        try:
            self.reset()
        except Exception:
            pass
        finally:
            # time cost is meaningless here
            manager.release_node(node, time_cost=0)

    return __wrap


class LibraStrictRedis(redis.StrictRedis):
    MANAGER_CALLBACKS = redis.StrictRedis.RESPONSE_CALLBACKS
    DEFAULT_EXC_CLASSES = (redis.ConnectionError, redis.TimeoutError)

    URL_TYPE_CONVERTER = {
        'socket_timeout': float,
        'socket_connect_timeout': float,
        'socket_keepalive': to_bool,
        'retry_on_timeout': to_bool,
        'decode_responses': to_bool,
        'socket_read_size': int,
        'max_connections': int,
    }

    @classmethod
    def from_url(cls, url, klass=redis.StrictRedis, **kwargs):
        parts = uritools.urisplit(url)
        if parts.query:
            query_args = parts.getquerylist()
            for key, value in query_args:
                if key in cls.URL_TYPE_CONVERTER:
                    fn = cls.URL_TYPE_CONVERTER[key]
                    value = fn(value)

                kwargs[key] = value

        # remove query string
        url = uritools.uricompose(
            scheme=parts.scheme, authority=parts.authority,
            path=parts.path, query='', fragment=parts.fragment,
        )
        return klass.from_url(url, **kwargs)

    def __init__(self, exc_classes=DEFAULT_EXC_CLASSES, max_retries=3, **kwargs):
        self.manager = kwargs.pop('manager', None)
        if self.manager is None:
            self.service_name = kwargs.pop('service_name')
            self.manager = WeightEndpoints(
                service_name=self.service_name,
                profile=kwargs.pop('profile', EtcdProfile.DEVELOP),
                recovery_num=kwargs.pop('recovery_num', 1000),
                **kwargs.pop('weight_kwargs', {})
            )
        else:
            self.service_name = kwargs.pop('service_name', None)
            self.service_name = self.service_name or getattr(self.manager, 'service_name', None)

        self.clients = {}
        self.client_kwargs = kwargs
        self.max_retries = max_retries
        self.exc_classes = exc_classes
        self.response_callbacks = self.__class__.MANAGER_CALLBACKS.copy()

    def get_client(self, node_uri):
        client = self.clients.get(node_uri)
        if not client:
            client = self.from_url(node_uri, **self.client_kwargs)
            self.clients[node_uri] = client

            # prepare client
            client.response_callbacks = self.response_callbacks

        return client

    def execute_command(self, *args, **options):
        failures = 0
        while True:
            try:
                return self._try_execute(*args, **options)
            except self.exc_classes:
                failures += 1
                if failures > self.max_retries:
                    raise

    def _try_execute(self, *args, **options):
        node_uri = self.manager.get_node()
        client = self.get_client(node_uri)
        LOGGER.debug('Got endpoint: %s', node_uri)
        start_time = time.time()
        try:
            result = client.execute_command(*args, **options)
        except self.exc_classes as err:
            LOGGER.info(
                'Endpoint [%s / %s] fails: %r, command: %s',
                self.service_name, node_uri, err, args[0])
            self.manager.dead_node(node_uri, time_cost=time.time() - start_time)
            LOGGER.error('Redis %s failed', node_uri)
            raise
        except Exception:
            self.manager.release_node(node_uri, time_cost=time.time() - start_time)
            raise
        else:
            self.manager.release_node(node_uri, time_cost=time.time() - start_time)
            return result

    def __repr__(self):
        return "%s{<%s>, %d clients%s%s%s}" % (
            type(self).__name__,
            self.service_name or 'unknown',
            len(self.clients),
            '\n\t' * bool(self.clients),
            '\n\t'.join([
                repr(client.connection_pool)
                for client in self.clients.values()
            ]), '\n' * bool(self.clients))

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        node_uri = self.manager.get_node()
        client = self.get_client(node_uri)

        pipeline = redis.client.StrictPipeline(
            client.connection_pool,
            client.response_callbacks,
            transaction,
            shard_hint)

        pipeline.__del__ = _patch_del(self.manager, node_uri)
        return pipeline

    def lock(self, name, timeout=None, sleep=0.1, blocking_timeout=None,
             lock_class=None, thread_local=True):
        raise NotImplementedError()

    def pubsub(self, **kwargs):
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        node_uri = self.manager.get_node()
        client = self.get_client(node_uri)

        pubsub = redis.client.PubSub(client.connection_pool, **kwargs)
        pubsub.__del__ = _patch_del(self.manager, node_uri)

        return pubsub
