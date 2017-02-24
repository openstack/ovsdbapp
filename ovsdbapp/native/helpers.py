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

import subprocess


def _connection_to_manager_uri(conn_uri):
    proto, addr = conn_uri.split(':', 1)
    if ':' in addr:
        ip, port = addr.split(':', 1)
        return 'p%s:%s:%s' % (proto, port, ip)
    else:
        return 'p%s:%s' % (proto, addr)


def enable_connection_uri(conn_uri, execute=None, **kwargs):
    timeout = kwargs.get('timeout', 5)
    man_uri = 'target="%s"' % _connection_to_manager_uri(conn_uri)
    cmd = ['ovs-vsctl', '--timeout=%d' % timeout, '--id=@manager', 'create',
           'Manager', man_uri, '--', 'add', 'Open_vSwitch', '.',
           'manager_options', '@manager']
    if execute:
        return execute(cmd, **kwargs).rstrip()
    else:
        obj = subprocess.Popen(['sudo'] + cmd, shell=False,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        out, err = obj.communicate()
        return out.rstrip()
