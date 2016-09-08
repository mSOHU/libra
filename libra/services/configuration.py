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
from collections import defaultdict

import deepdiff

from libra.watcher import Watcher
from libra.utils import get_conf, EtcdProfile, Undefined


logger = logging.getLogger(__name__)


class Configuration(object):
    CONFIG_PATH = '/config/%s/spec'
    LEVEL_SEP = '.'
    ALLOWED_SPEC = 'v1'

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
        self.watch_functions = defaultdict(list)
        self.value_cache = {}

    def _on_config_change(self, value, **_):
        new_spec = json.loads(value)
        assert 'version' in new_spec and \
               new_spec['version'] == self.ALLOWED_SPEC, 'unacceptable version'
        assert 'spec' in new_spec and \
               isinstance(new_spec['spec'], dict), 'invalid spec'

        old_config, new_config = self.current_config, new_spec['spec']
        if old_config is None:
            logger.info(
                u'Config `%s` loaded: \n%s',
                self.config_name, pprint.pformat(new_config))
            self.current_config = new_config
            self.ready_event.set()
            self._dispatch_init_watchers()
        else:
            diffs = self.locate_differences(old_config or {}, new_config)
            if diffs:
                self._update_value_cache(diffs)
                self.current_config = new_config
                self._dispatch_watchers(diffs)
                self.log_different(diffs)

    def _on_config_init(self, root):
        for node in root.leaves:
            if node.key == self.watch_path:
                self._on_config_change(value=node.value)

    DIFF_PATTERN = re.compile(r"\['?([^'\]]+)'?\]")
    DIFF_MAP = {
        'dictionary_item_added': 'ADDED',
        'dictionary_item_removed': 'REMOVED',
        'iterable_item_added': 'ADDED',
        'iterable_item_removed': 'REMOVED',
        'values_changed': 'CHANGED'
    }

    def locate_differences(self, old, new):
        result = []

        for typ_name, diffs in deepdiff.DeepDiff(old, new, verbose_level=2).items():
            event_name = self.DIFF_MAP[typ_name]
            for location, value in diffs.items():
                full_path = '.'.join(self.DIFF_PATTERN.findall(location[4:]))
                if event_name == 'ADDED':
                    old_value, new_value = Undefined, value
                elif event_name == 'REMOVED':
                    old_value, new_value = value, Undefined
                else:
                    old_value, new_value = value['old_value'], value['new_value']

                result.append((event_name, full_path, old_value, new_value))

        return result

    def log_different(self, diffs):
        diff_texts = defaultdict(list)
        for event_name, full_path, old_value, new_value in sorted(diffs):
            if event_name == 'ADDED':
                old_value, new_value = '', repr(new_value)
            elif event_name == 'REMOVED':
                old_value, new_value = repr(old_value), 'x'
            else:
                old_value, new_value = repr(old_value), repr(new_value)

            diff_texts[event_name].append('\t'.join(['[%s]' % full_path, old_value, '->', new_value]))

        result_text = ''.join([
            '%s:\n\t%s\n' % (event_name, '\n\t'.join(diffs))
            for event_name, diffs in diff_texts.items()
        ])
        logger.warning(
            u'Config `%s` changed: \n%s',
            self.config_name, result_text)

    def __getitem__(self, item):
        self.ready_event.wait()

        if item in self.value_cache:
            return self.value_cache[item]

        value = self.value_cache[item] = get_conf(
            name=item, sep=self.LEVEL_SEP,
            conf=self.current_config)
        return value

    def _update_value_cache(self, diffs):
        for event_name, event_path, old_value, new_value in diffs:
            # if some path has been changed or removed,
            # it maybe changes leaves values,
            # so we remove all leaves under the path
            event_path_prefix = '%s.' % event_path
            for item_path in self.value_cache.keys():
                if item_path.startswith(event_path_prefix):
                    del self.value_cache[item_path]

            paths = event_path.split(self.LEVEL_SEP)
            for i in range(len(paths)):
                level_path = self.LEVEL_SEP.join(paths[:i])
                if level_path in self.value_cache:
                    del self.value_cache[level_path]

            if event_path in self.value_cache:
                if event_name == 'CHANGED':
                    self.value_cache[event_path] = new_value
                elif event_name == 'REMOVED':
                    del self.value_cache[event_path]

    def watch(self, watch_path):
        def _wrapper(fn):
            self.watch_functions[watch_path].append(fn)
            return fn
        return _wrapper

    def _dispatch_init_watchers(self):
        for watch_path, funcs in self.watch_functions.items():
            for fn in funcs:
                try:
                    new_value = self[watch_path]
                except KeyError:
                    new_value = Undefined

                try:
                    fn(event_name='INIT', old_value=Undefined, new_value=new_value)
                except Exception as err:
                    logger.exception(
                        '%r, while invoking init config watcher: [%s] %r',
                        err, watch_path, fn)

    def _dispatch_watchers(self, diffs):
        triggered_paths = set()
        for event_name, event_path, old_value, new_value in diffs:
            for watch_path, funcs in self.watch_functions.items():
                # don't trigger multiple times
                if watch_path in triggered_paths:
                    continue

                watch_prefix = '%s.' % watch_path
                if event_path == watch_path or event_path.startswith(watch_prefix):
                    triggered_paths.add(watch_path)

                    # if only a prefixed path, try to obtain the value on watched path
                    if watch_path != event_path:
                        try:
                            new_value = self[watch_path]
                        except KeyError:
                            new_value = Undefined

                    for watcher_fn in funcs:
                        try:
                            watcher_fn(event_name=event_name, old_value=old_value, new_value=new_value)
                        except Exception as err:
                            logger.exception(
                                '%r, while invoking config watcher: %r', err, watcher_fn)

    def __setitem__(self, key, value):
        raise NotImplementedError()
