#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'johnxu'
__date__ = '2015/5/12 16:09'


SAME_SECTION_WEIGHT = 100

# same IP section       100
# same IP section 2     60
# same ISP              10
# diff ISP              5
# long ISP              1


def bidirection_dict(d):
    d = d.copy()
    for (k1, k2), v in d.items():
        d[k2, k1] = v

    return d


def ip_section(ip, depth=2):
    return '.'.join(ip.split('.')[:depth])


def calc_weight(local_ip, remote_nodes, weight_map,
                directional_weight, section_depth=3):
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


