# Copyright (c) 2016 Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import logging

from ovs import vlog

_LOG = logging.getLogger(__name__)

# Map local log LEVELS to local LOG functions
CRITICAL = _LOG.critical
ERROR = _LOG.error
WARN = _LOG.warn
INFO = _LOG.info
DEBUG = _LOG.debug

_LOG_MAPPING = collections.OrderedDict((
    (CRITICAL, vlog.Vlog.emer),
    (ERROR, vlog.Vlog.err),
    (WARN, vlog.Vlog.warn),
    (INFO, vlog.Vlog.info),
    (DEBUG, vlog.Vlog.dbg),
))
ALL_LEVELS = tuple(_LOG_MAPPING.keys())


def _original_vlog_fn(level):
    """Get the original unpatched OVS vlog function for level"""
    return _LOG_MAPPING[level]


def _current_vlog_fn(level):
    """Get the currently used OVS vlog function mapped to level"""
    return getattr(vlog.Vlog, _LOG_MAPPING[level].__name__)


def use_python_logger(levels=ALL_LEVELS, max_level=None):
    """Replace the OVS vlog functions with our logger

    :param: levels: log levels *from this module* e.g. [vlog.WARN]
    :type: levels: iterable
    :param: max_level: the maximum level to log
    :type: max_level: vlog level, CRITICAL, ERROR, WARN, INFO, or DEBUG
    """

    if max_level:
        levels = levels[:-levels.index(max_level)]

    # NOTE(twilson) Replace functions directly instead of subclassing so that
    # debug messages contain the correct function/filename/line information
    for log in levels:
        setattr(vlog.Vlog, _LOG_MAPPING[log].__name__, log)


def reset_logger():
    """Reset the OVS vlog functions to their original values"""
    for log in ALL_LEVELS:
        setattr(vlog.Vlog, _LOG_MAPPING[log].__name__, _LOG_MAPPING[log])


def is_patched(level):
    """Test if the vlog level is patched"""
    return _current_vlog_fn(level) != _original_vlog_fn(level)
