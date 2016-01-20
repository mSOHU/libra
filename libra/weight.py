#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'johnxu'
__date__ = '2015/5/12 16:09'


SAME_SECTION_WEIGHT = 1000
REPLICA_WEIGHT_MAP = {
    ('10.10', '192.168'): 400,
    ('10.10', '10.13'): 250,
    ('10.10', '10.16'): 1,

    ('10.13', '192.168'): 1000,
    ('10.13', '10.16'): 1,
    ('192.168', '10.16'): 1,
}
REPLICA_DIRECTIONAL_WEIGHT = {
    ('10.16.19', '10.13.82'): 10,
    ('10.13', '10.10'): 1,
}


def bidirection_dict(d):
    d = d.copy()
    for (k1, k2), v in d.items():
        d[k2, k1] = v

    return d


def ip_section(ip, depth=2):
    return '.'.join(ip.split('.')[:depth])


def calc_weight(local_ip, remote_nodes,
                weight_map=REPLICA_WEIGHT_MAP,
                directional_weight=REPLICA_DIRECTIONAL_WEIGHT,
                section_depth=3):
    weight_map = bidirection_dict(weight_map)
    weight_map.update(directional_weight)

    def _weight(node_ip):
        for depth in range(section_depth, 0, -1):
            try:
                return weight_map[ip_section(local_ip, depth), ip_section(node_ip, depth)]
            except KeyError:
                if ip_section(local_ip, depth) == ip_section(node_ip, depth):
                    return SAME_SECTION_WEIGHT

                continue
        else:
            raise RuntimeError('no acceptable weight in [%s, %s]' % (local_ip, node_ip))

    return {
        node: _weight(node_ip)
        for node_ip, node in remote_nodes.items()
    }


