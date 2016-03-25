#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 1/19/2016 6:22 PM
"""

import os
import random
import socket
import logging
import functools

import etcd
import yaml


PKG_PATH = ROOT_PATH = os.path.join(os.path.dirname(__file__))
make_path = functools.partial(os.path.join, ROOT_PATH)
make_pkg_path = functools.partial(os.path.join, PKG_PATH)


_CONFIG = yaml.load(open(make_pkg_path('conf/config.yaml'), 'rb').read())


def get_conf(name=None, sep='.', conf=None):
    conf = conf or _CONFIG
    if conf is None:
        raise RuntimeError('config not initialized')

    def _get_conf(path, config):
        return _get_conf(path[1:], config[path[0]]) if path else config
    return _get_conf(name.split(sep), conf) if name else conf


_ETCD_CLIENT = None


def get_etcd():
    """
    :rtype: etcd.Client
    """
    global _ETCD_CLIENT
    if _ETCD_CLIENT is None:
        servers = [(host, port) for host, port in get_conf('etcd.server')]
        random.shuffle(servers)
        _ETCD_CLIENT = etcd.Client(
            tuple(servers), **get_conf('etcd.settings')
        )
    return _ETCD_CLIENT


def init_logging(standalone=False, module_name='libra'):
    if standalone:
        formatter = logging.Formatter(
            '[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)

        logger = logging.getLogger(module_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.addHandler(handler)

    # hide INFO level for prevent meaningless `Resetting dropped connection`
    # for every watch request with a timeout must triggers.
    logger = logging.getLogger('urllib3.connectionpool')
    logger.setLevel(logging.WARNING)


RR_COUNTER = int(random.random() * 1000)


def rr_next(modulus=1):
    global RR_COUNTER
    RR_COUNTER += 1
    return RR_COUNTER % modulus


def rr_choice(choices):
    return choices[rr_next(len(choices))]


_UTF8_TYPES = (bytes, type(None))


def utf8(value):
    """Converts a string argument to a byte string.

    If the argument is already a byte string or None, it is returned unchanged.
    Otherwise it must be a unicode string and is encoded as utf8.
    """
    if isinstance(value, _UTF8_TYPES):
        return value
    if not isinstance(value, unicode):
        raise TypeError(
            "Expected bytes, unicode, or None; got %r" % type(value)
        )
    return value.encode("utf-8")


def local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('10.10.10.10', 80))
    ip_addr = s.getsockname()[0]
    s.close()

    return ip_addr
