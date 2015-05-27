# coding: utf-8

__author__ = 'johnxu'

import functools

from tornado import simple_httpclient


class LibraAsyncHTTPClient(simple_httpclient.SimpleAsyncHTTPClient):
    def __init__(self, manager, placeholder='__node__'):
        self.manager = manager
        self.placeholder = placeholder

    def initialize(self, io_loop=None, max_clients=10,
                   hostname_mapping=None, max_buffer_size=104857600):
        super(LibraAsyncHTTPClient, self).initialize(
            io_loop=io_loop, max_clients=10,
            hostname_mapping=None, max_buffer_size=104857600)

    def fetch(self, request, callback, **kwargs):
        @functools.wraps(callback)
        def wrapper(response):
            if response.error:
                self.manager.dead_node(node, response.request_time)
            else:
                self.manager.release_node(node, response.request_time)
            return callback(response)

        node = self.manager.get_node()
        request.url = request.url.replace(self.placeholder, node)
        return super(LibraAsyncHTTPClient, self).fetch(request, wrapper, **kwargs)
