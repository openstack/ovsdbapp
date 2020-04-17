# Copyright (c) 2015 Red Hat, Inc.
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

import logging
import subprocess

LOG = logging.getLogger(__name__)


def _connection_to_manager_uri(conn_uri):
    proto, addr = conn_uri.split(':', 1)
    if ':' in addr:
        ip, port = addr.split(':', 1)
        return 'p%s:%s:%s' % (proto, port, ip)
    return 'p%s:%s' % (proto, addr)


# TODO(jlibosva): Get rid of this runtime configuration and raise a message to
#                 set Manager outside of ovsdbapp.
def enable_connection_uri(conn_uri, execute=None, **kwargs):
    timeout = kwargs.pop('timeout', 5)
    # NOTE(ralonsoh): the command timeout , "timeout", is defined in seconds;
    # the probe timeout is defined in milliseconds. If "timeout" is used, must
    # be converted to ms.
    probe = (timeout * 1000 if kwargs.pop('set_timeout', None) else
             kwargs.pop('inactivity_probe', None))
    man_uri = _connection_to_manager_uri(conn_uri)
    cmd = ['ovs-vsctl', '--timeout=%d' % timeout, '--id=@manager',
           '--', 'create', 'Manager', 'target="%s"' % man_uri,
           '--', 'add', 'Open_vSwitch', '.', 'manager_options', '@manager']
    if probe is not None:
        cmd += ['--', 'set', 'Manager', man_uri, 'inactivity_probe=%s' % probe]
    if execute:
        return execute(cmd, **kwargs).rstrip()
    else:
        obj = subprocess.Popen(['sudo'] + cmd, shell=False,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        out, err = obj.communicate()
        if err:
            LOG.debug(err)  # will fail if target already exists
        return out.rstrip()
