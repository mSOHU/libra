#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 9/2/2016 4:27 PM
"""

import time

from libra.utils import init_logging, EtcdProfile
from libra.services.configuration import Configuration


init_logging(standalone=True)
PROFILE = EtcdProfile.DEVELOP

config = Configuration('wcms_front', profile=EtcdProfile.DEVELOP)

while True:
    time.sleep(1)
    print config['CACHE_SERVERS.coop']
