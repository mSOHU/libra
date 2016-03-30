#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/24/2016 5:40 PM
"""

from libra.endpoint import EndpointWatcher
from libra.utils import init_logging, rr_choice


init_logging(standalone=True)


def switch_callback(**kwargs):
    print kwargs


EndpointWatcher('test', 'all', switch_callback)


def switch_callback_choice(**kwargs):
    print kwargs
    return rr_choice(kwargs['endpoint_list'])


EndpointWatcher('test', 'choice', switch_callback_choice)

raw_input('')
