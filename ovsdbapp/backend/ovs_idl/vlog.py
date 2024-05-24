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
import sys

from ovs import vlog

try:
    from eventlet import patcher
    # If eventlet is installed and the 'thread' module is patched, we will
    # skip setting up the python logger on Windows.
    EVENTLET_NONBLOCKING_MODE_ENABLED = patcher.is_monkey_patched('thread')
except ImportError:
    EVENTLET_NONBLOCKING_MODE_ENABLED = False

_LOG = logging.getLogger(__name__)

# Map local log LEVELS to local LOG functions
CRITICAL = _LOG.critical
ERROR = _LOG.error
WARN = _LOG.warning
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
    if sys.platform == 'win32' and EVENTLET_NONBLOCKING_MODE_ENABLED:
        # NOTE(abalutoiu) When using oslo logging we need to keep in mind that
        # it does not work well with native threads. We need to be careful when
        # we call eventlet.tpool.execute, and make sure that it will not use
        # the oslo logging, since it might cause unexpected hangs if
        # greenthreads are used. On Windows we have to use
        # eventlet.tpool.execute for a call to the ovs lib which will use
        # vlog to log messages. We will skip replacing the OVS IDL logger
        # functions on Windows to avoid unexpected hangs with oslo logging
        return

    if max_level:
        levels = levels[:levels.index(max_level) + 1]

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
