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

import glob
import os
import shutil
import signal
import subprocess
import time

import fixtures


class OvsVenvFixture(fixtures.Fixture):
    def __init__(self, venv, ovsdir, dummy=None, remove=False):
        if not os.path.isdir(ovsdir):
            raise Exception("%s is not a directory" % ovsdir)
        self.venv = venv
        self.ovsdir = ovsdir
        self._dummy = dummy
        self.remove = remove
        self.env = {'OVS_RUNDIR': self.venv, 'OVS_LOGDIR': self.venv,
                    'OVS_DBDIR': self.venv, 'OVS_SYSCONFDIR': self.venv,
                    'PATH': "{0}/ovsdb:{0}/vswitchd:{0}/utilities:{0}/vtep:"
                            "{0}/ovn/controller:{0}/ovn/controller-vtep:"
                            "{0}/ovn/northd:{0}/ovn/utilities:{1}:".format(
                                self.ovsdir, os.getenv('PATH'))}

    @property
    def ovs_schema(self):
        return os.path.join(self.ovsdir, 'vswitchd', 'vswitch.ovsschema')

    @property
    def ovnsb_schema(self):
        return os.path.join(self.ovsdir, 'ovn', 'ovn-sb.ovsschema')

    @property
    def ovnnb_schema(self):
        return os.path.join(self.ovsdir, 'ovn', 'ovn-nb.ovsschema')

    @property
    def vtep_schema(self):
        return os.path.join(self.ovsdir, 'vtep', 'vtep.ovsschema')

    @property
    def dummy_arg(self):
        if self._dummy == 'override':
            return "--enable-dummy=override"
        elif self._dummy == 'system':
            return "--enable-dummy=system"
        else:
            return "--enable-dummy="

    @property
    def ovs_connection(self):
        return 'unix:' + os.path.join(self.venv, 'db.sock')

    @property
    def ovnnb_connection(self):
        return 'unix:' + os.path.join(self.venv, 'ovnnb_db.sock')

    @property
    def ovnsb_connection(self):
        return 'unix:' + os.path.join(self.venv, 'ovnsb_db.sock')

    def _setUp(self):
        self.addCleanup(self.deactivate)
        if not os.path.isdir(self.venv):
            os.mkdir(self.venv)
        self.create_db('conf.db', self.ovs_schema)
        self.create_db('ovnsb.db', self.ovnsb_schema)
        self.create_db('ovnnb.db', self.ovnnb_schema)
        self.create_db('vtep.db', self.vtep_schema)
        self.call(['ovsdb-server',
                   '--remote=p' + self.ovs_connection,
                   '--detach', '--no-chdir', '--pidfile', '-vconsole:off',
                   '--log-file', 'vtep.db', 'conf.db'])
        self.call(['ovsdb-server', '--detach', '--no-chdir', '-vconsole:off',
                   '--pidfile=%s' % os.path.join(self.venv, 'ovnnb_db.pid'),
                   '--log-file=%s' % os.path.join(self.venv, 'ovnnb_db.log'),
                   '--remote=db:OVN_Northbound,NB_Global,connections',
                   '--private-key=db:OVN_Northbound,SSL,private_key',
                   '--certificate=db:OVN_Northbound,SSL,certificate',
                   '--ca-cert=db:OVN_Northbound,SSL,ca_cert',
                   '--ssl-protocols=db:OVN_Northbound,SSL,ssl_protocols',
                   '--ssl-ciphers=db:OVN_Northbound,SSL,ssl_ciphers',
                   '--remote=p' + self.ovnnb_connection, 'ovnnb.db'])
        self.call(['ovsdb-server', '--detach', '--no-chdir', '-vconsole:off',
                   '--pidfile=%s' % os.path.join(self.venv, 'ovnsb_db.pid'),
                   '--log-file=%s' % os.path.join(self.venv, 'ovnsb_db.log'),
                   '--remote=db:OVN_Southbound,SB_Global,connections',
                   '--private-key=db:OVN_Southbound,SSL,private_key',
                   '--certificate=db:OVN_Southbound,SSL,certificate',
                   '--ca-cert=db:OVN_Southbound,SSL,ca_cert',
                   '--ssl-protocols=db:OVN_Southbound,SSL,ssl_protocols',
                   '--ssl-ciphers=db:OVN_Southbound,SSL,ssl_ciphers',
                   '--remote=p' + self.ovnsb_connection, 'ovnsb.db'])
        time.sleep(1)  # wait_until_true(os.path.isfile(db_sock)
        self.call(['ovs-vsctl', '--no-wait', '--', 'init'])
        self.call(['ovs-vswitchd', '--detach', '--no-chdir', '--pidfile',
                   '-vconsole:off', '-vvconn', '-vnetdev_dummy', '--log-file',
                   self.dummy_arg, self.ovs_connection])
        self.call(['ovn-nbctl', 'init'])
        self.call(['ovn-sbctl', 'init'])
        self.call([
            'ovs-vsctl', 'set', 'open', '.',
            'external_ids:system-id=56b18105-5706-46ef-80c4-ff20979ab068',
            'external_ids:hostname=sandbox',
            'external_ids:ovn-encap-type=geneve',
            'external_ids:ovn-encap-ip=127.0.0.1'])
        # TODO(twilson) SSL stuff
        if False:
            pass
        else:
            self.call(['ovs-vsctl', 'set', 'open', '.',
                       'external_ids:ovn-remote=' + self.ovnsb_connection])
        self.call(['ovn-northd', '--detach', '--no-chdir', '--pidfile',
                   '-vconsole:off', '--log-file',
                   '--ovnsb-db=' + self.ovnsb_connection,
                   '--ovnnb-db=' + self.ovnnb_connection])
        self.call(['ovn-controller', '--detach', '--no-chdir', '--pidfile',
                   '-vconsole:off', '--log-file'])
        self.call(['ovn-controller-vtep', '--detach', '--no-chdir',
                   '--pidfile', '-vconsole:off', '--log-file',
                   '--ovnsb-db=' + self.ovnsb_connection])

    def deactivate(self):
        self.kill_processes()
        if self.remove:
            shutil.rmtree(self.venv, ignore_errors=True)

    def create_db(self, name, schema):
        filename = os.path.join(self.venv, name)
        if not os.path.isfile(filename):
            return self.call(['ovsdb-tool', '-v', 'create', name, schema])

    def call(self, *args, **kwargs):
        cwd = kwargs.pop('cwd', self.venv)
        return subprocess.check_call(*args, env=self.env, cwd=cwd, **kwargs)

    def get_pids(self):
        files = glob.glob(os.path.join(self.venv, "*.pid"))
        result = []
        for fname in files:
            with open(fname, 'r') as f:
                result.append(int(f.read().strip()))
        return result

    def kill_processes(self):
        for pid in self.get_pids():
            os.kill(pid, signal.SIGTERM)
