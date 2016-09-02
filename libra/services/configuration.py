#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 9/2/2016 4:13 PM
"""

import json
import threading

from libra.utils import get_conf, EtcdProfile
from libra.watcher import Watcher


class Configuration(object):
    CONFIG_PATH = '/config/%s/spec'
    LEVEL_SEP = '.'
    ALLOWED_SPEC = 'v1'

    def __init__(self, project_name, profile):
        """
        :type profile: EtcdProfile
        """
        self.project_name = project_name
        self.profile = profile
        self.watch_path = self.CONFIG_PATH % self.project_name

        self.watcher = Watcher(
            self.watch_path, profile=self.profile,
            change_callback=self.on_config_change,
            init_callback=self.on_config_init,
            final_state=True,
        )
        self.ready_event = threading.Event()
        self.current_config = None

    def on_config_change(self, value, **_):
        new_config = json.loads(value)
        assert 'version' in new_config and \
               new_config['version'] == self.ALLOWED_SPEC, 'unacceptable version'
        assert 'spec' in new_config and \
               isinstance(new_config['spec'], dict), 'invalid spec'
        self.current_config = new_config['spec']
        self.ready_event.set()

    def on_config_init(self, root):
        for node in root.leaves:
            if node.key == self.watch_path:
                self.on_config_change(value=node.value)

    def __getitem__(self, item):
        self.ready_event.wait()
        return get_conf(
            name=item, sep=self.LEVEL_SEP,
            conf=self.current_config)

    def __setitem__(self, key, value):
        raise NotImplementedError()
