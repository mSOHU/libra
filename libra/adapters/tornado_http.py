# coding: utf-8

__author__ = 'johnxu'

import logging
import functools

from tornado import simple_httpclient


LOGGER = logging.getLogger(__name__)


class LibraAsyncHTTPClient(simple_httpclient.SimpleAsyncHTTPClient):
    def initialize(self, io_loop=None, max_clients=10,
                   hostname_mapping=None, max_buffer_size=104857600,
                   manager=None, placeholder='__node__'):
        super(LibraAsyncHTTPClient, self).initialize(
            io_loop=io_loop, max_clients=10,
            hostname_mapping=None, max_buffer_size=104857600)

        self.manager = manager
        self.placeholder = placeholder

    def fetch(self, request, callback, **kwargs):
        @functools.wraps(callback)
        def wrapper(response):
            if response.error:
                LOGGER.error('LIBRA: dead node, %s, %ss', node, response.request_time)
                self.manager.dead_node(node, response.request_time)
            else:
                LOGGER.debug('LIBRA: release node, %s, %ss', node, response.request_time)
                self.manager.release_node(node, response.request_time)
            return callback(response)

        node = self.manager.get_node()
        LOGGER.debug('LIBRA: got node, %s', node)
        request.url = request.url.replace(self.placeholder, node)
        return super(LibraAsyncHTTPClient, self).fetch(request, wrapper, **kwargs)
