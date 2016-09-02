#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 9/2/2016 4:13 PM
"""

import re
import json
import pprint
import logging
import threading

import deepdiff

from libra.utils import get_conf, EtcdProfile
from libra.watcher import Watcher


logger = logging.getLogger(__name__)


class Configuration(object):
    CONFIG_PATH = '/config/%s/spec'
    LEVEL_SEP = '.'
    ALLOWED_SPEC = 'v1'
    DIFF_PATTERN = re.compile(r"\['?([^'\]]+)'?\]")

    def __init__(self, config_name, profile):
        """
        :type profile: EtcdProfile
        """
        self.config_name = config_name
        self.profile = profile
        self.watch_path = self.CONFIG_PATH % self.config_name

        self.watcher = Watcher(
            self.watch_path, profile=self.profile,
            change_callback=self._on_config_change,
            init_callback=self._on_config_init,
            final_state=True,
        )
        self.ready_event = threading.Event()
        self.current_config = None

    def _on_config_change(self, value, **_):
        new_config = json.loads(value)
        assert 'version' in new_config and \
               new_config['version'] == self.ALLOWED_SPEC, 'unacceptable version'
        assert 'spec' in new_config and \
               isinstance(new_config['spec'], dict), 'invalid spec'

        self.log_different(self.current_config, new_config['spec'])
        self.current_config = new_config['spec']
        self.ready_event.set()

    def _on_config_init(self, root):
        for node in root.leaves:
            if node.key == self.watch_path:
                self._on_config_change(value=node.value)

    DIFF_MAP = [
        ('iterable_item_removed', 'REMOVED'),
        ('iterable_item_added', 'ADDED'),
        ('dictionary_item_removed', 'REMOVED'),
        ('dictionary_item_added', 'ADDED'),
        ('values_changed', 'CHANGED'),
    ]

    def log_different(self, old, new):
        if old is None:
            logger.info(
                'Config `%s` loaded: \n%s',
                self.config_name, pprint.pformat(new))
            return

        diff_texts = []
        differences = deepdiff.DeepDiff(old, new, verbose_level=2)
        for typ, name in self.DIFF_MAP:
            diff = differences.get(typ, {})
            if not diff:
                continue

            type_diff = []
            diff_texts.append((name, type_diff))

            for location, value in diff.items():
                full_path = '.'.join(self.DIFF_PATTERN.findall(location[4:]))
                if name == 'ADDED':
                    old_value, new_value = '', repr(value)
                elif name == 'REMOVED':
                    old_value, new_value = repr(value), 'x'
                else:
                    old_value, new_value = repr(value['old_value']), repr(value['new_value'])

                type_diff.append('\t'.join(['[%s]' % full_path, old_value, '->', new_value]))

        result_text = ''.join([
            '%s:\n\t%s\n' % (name, '\n\t'.join(diffs))
            for name, diffs in diff_texts
        ])
        logger.warning(
            'Config `%s` changed: \n%s',
            self.config_name, result_text)

    def __getitem__(self, item):
        self.ready_event.wait()
        return get_conf(
            name=item, sep=self.LEVEL_SEP,
            conf=self.current_config)

    def __setitem__(self, key, value):
        raise NotImplementedError()
