#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/24/2016 5:40 PM
"""

from libra.endpoint import EndpointWatcher, SwitchStrategy
from libra.utils import init_logging, rr_choice, EtcdProfile


init_logging(standalone=True)
PROFILE = EtcdProfile.DEVELOP


def switch_callback(**kwargs):
    print kwargs


EndpointWatcher(
    service_name='test',
    profile=PROFILE,
    strategy=SwitchStrategy.ANY,
    switch_callback=switch_callback
)


def switch_callback_chosen(**kwargs):
    print kwargs
    return rr_choice(kwargs['endpoint_list'])


EndpointWatcher(
    service_name='test',
    profile=PROFILE,
    strategy=SwitchStrategy.CHOSEN,
    switch_callback=switch_callback_chosen
)

raw_input('')
