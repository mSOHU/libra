# coding: utf-8

"""
@author: johnxu
@date: 3/8/2016 6:34 PM
"""

import logging

import http2
from tornado import httpclient


LOGGER = logging.getLogger(__name__)


class LibraAsyncHTTP2Client(object):
    def __init__(self, manager=None, placeholder='__node__', **conn_kwargs):
        self.manager = manager
        self.placeholder = placeholder

        conn_kwargs.pop('host', None)
        conn_kwargs['force_instance'] = True
        self.conn_kwargs = conn_kwargs
        self.clients = {
            node: http2.SimpleAsyncHTTP2Client(host=node, **self.conn_kwargs)
            for node in self.manager._weight_node
        }

    def fetch(self, request, callback, **kwargs):
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
        return self.clients[node].fetch(request, wrapper, **kwargs)
