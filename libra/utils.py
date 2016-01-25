#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 1/19/2016 6:22 PM
"""

import os
import random
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
