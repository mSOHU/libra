# coding: utf-8

"""
@author: johnxu
@date: 3/8/2016 6:34 PM
"""

import logging

import http2
from tornado import httpclient
from libra.utils import extract_netloc


LOGGER = logging.getLogger(__name__)


class LibraAsyncHTTP2Client(object):
    def __init__(self, manager=None, **conn_kwargs):
        self.manager = manager

        conn_kwargs.pop('host', None)
        conn_kwargs['force_instance'] = True
        self.conn_kwargs = conn_kwargs
        self.clients = {}

    def fetch(self, request, callback=None, **kwargs):
        def wrapper(response):
            error = response.error
            if error and isinstance(error, httpclient.HTTPError) and error.code == 599:
                LOGGER.warning('LIBRA: dead node, %s, %.2fs', node, response.request_time)
                self.manager.dead_node(node, response.request_time)
            else:
                LOGGER.debug('LIBRA: release node, %s, %ss', node, response.request_time)
                self.manager.release_node(node, response.request_time)
            return callback and callback(response)

        node = self.manager.get_node()
        LOGGER.debug('LIBRA: got node, %s', node)
        client = self.clients.get(node)
        if not client:
            node_ip = extract_netloc(node)
            client = http2.SimpleAsyncHTTP2Client(host=node_ip, **self.conn_kwargs)
            self.clients[node] = client
        return client.fetch(request, wrapper, **kwargs)
