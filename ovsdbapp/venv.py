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

# These are the valid dummy values for ovs-vswitchd process. They are here just
# to get user enumeration. See man ovs-vswitchd(8) for more information.
DUMMY_OVERRIDE_ALL = 'override'
DUMMY_OVERRIDE_SYSTEM = 'system'
DUMMY_OVERRIDE_NONE = ''


class OvsVenvFixture(fixtures.Fixture):
    PATH_VAR_TEMPLATE = "{0}/ovsdb:{0}/vswitchd:{0}/utilities"
    OVS_PATHS = (
        os.path.join(os.path.sep, 'usr', 'local', 'share', 'openvswitch'),
        os.path.join(os.path.sep, 'usr', 'share', 'openvswitch'))

    def __init__(self, venv, ovsdir=None, dummy=DUMMY_OVERRIDE_ALL,
                 remove=False):
        """Initialize fixture

        :param venv: Path to venv directory.
        :param ovsdir: Path to directory containing ovs source codes.
        :param dummy: One of following: an empty string, 'override' or
                      'system'.
        :param remove: Boolean value whether venv directory should be removed
                       at the fixture cleanup.
        """
        self.venv = venv
        self.env = {'OVS_RUNDIR': self.venv, 'OVS_LOGDIR': self.venv,
                    'OVS_DBDIR': self.venv, 'OVS_SYSCONFDIR': self.venv}
        if ovsdir and os.path.isdir(ovsdir):
            # From source directory
            self.env['PATH'] = (self.PATH_VAR_TEMPLATE.format(ovsdir) +
                                ":%s" % os.getenv('PATH'))
        else:
            # Use installed OVS
            self.env['PATH'] = os.getenv('PATH')

        self.ovsdir = self._share_path(self.OVS_PATHS, ovsdir)
        self._dummy = dummy
        self.remove = remove
        self.ovsdb_server_dbs = []

    @staticmethod
    def _share_path(paths, override=None, files=tuple()):
        if not override:
            try:
                return next(
                    p for p in paths if os.path.isdir(p) and
                    all(os.path.isfile(os.path.join(p, f)) for f in files))
            except StopIteration:
                pass
        elif os.path.isdir(override):
            return override

        raise Exception("Invalid directories: %s" %
                        ", ".join(paths + (str(override),)))

    @property
    def ovs_schema(self):
        path = os.path.join(self.ovsdir, 'vswitchd', 'vswitch.ovsschema')
        if os.path.isfile(path):
            return path
        return os.path.join(self.ovsdir, 'vswitch.ovsschema')

    @property
    def dummy_arg(self):
        return "--enable-dummy=%s" % self._dummy

    @property
    def ovs_connection(self):
        return 'unix:' + os.path.join(self.venv, 'db.sock')

    def _setUp(self):
        super(OvsVenvFixture, self)._setUp()
        self.addCleanup(self.deactivate)
        if not os.path.isdir(self.venv):
            os.mkdir(self.venv)
        self.setup_dbs()
        self.start_ovsdb_processes()
        time.sleep(1)  # wait_until_true(os.path.isfile(db_sock)
        self.init_processes()

    def setup_dbs(self):
        db_filename = 'conf.db'
        self.create_db(db_filename, self.ovs_schema)
        self.ovsdb_server_dbs.append(db_filename)

    def start_ovsdb_processes(self):
        self.call([
            'ovsdb-server',
            '--remote=p' + self.ovs_connection,
            '--detach', '--no-chdir', '--pidfile', '-vconsole:off',
            '--log-file'] + self.ovsdb_server_dbs)

    def init_processes(self):
        self.call(['ovs-vsctl', '--no-wait', '--', 'init'])
        self.call(['ovs-vswitchd', '--detach', '--no-chdir', '--pidfile',
                   '-vconsole:off', '-vvconn', '-vnetdev_dummy', '--log-file',
                   self.dummy_arg, self.ovs_connection])

    def deactivate(self):
        self.kill_processes()
        if self.remove:
            shutil.rmtree(self.venv, ignore_errors=True)

    def create_db(self, name, schema):
        filename = os.path.join(self.venv, name)
        if not os.path.isfile(filename):
            return self.call(['ovsdb-tool', '-v', 'create', name, schema])

    def call(self, cmd, *args, **kwargs):
        cwd = kwargs.pop('cwd', self.venv)
        return subprocess.check_call(
            cmd, *args, env=self.env, stderr=subprocess.STDOUT,
            cwd=cwd, **kwargs)

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


class OvsOvnVenvFixture(OvsVenvFixture):
    OVN_PATHS = (
        os.path.join(os.path.sep, 'usr', 'local', 'share', 'ovn'),
        os.path.join(os.path.sep, 'usr', 'share', 'ovn')) + (
            OvsVenvFixture.OVS_PATHS)
    NBSCHEMA = 'ovn-nb.ovsschema'
    SBSCHEMA = 'ovn-sb.ovsschema'

    def __init__(self, venv, ovndir=None, add_chassis=False, **kwargs):
        self.add_chassis = add_chassis
        if ovndir and os.path.isdir(ovndir):
            # Use OVN source dir
            self.PATH_VAR_TEMPLATE += (
                ":{0}/controller:{0}/northd:{0}/utilities".format(ovndir))
        super(OvsOvnVenvFixture, self).__init__(venv, **kwargs)
        self.ovndir = self._share_path(self.OVN_PATHS, ovndir,
                                       [self.SBSCHEMA, self.NBSCHEMA])
        self.env.update({'OVN_RUNDIR': self.venv})

    @property
    def ovnsb_schema(self):
        return os.path.join(self.ovndir, self.SBSCHEMA)

    @property
    def ovnnb_schema(self):
        return os.path.join(self.ovndir, self.NBSCHEMA)

    @property
    def ovnnb_connection(self):
        return 'unix:' + os.path.join(self.venv, 'ovnnb_db.sock')

    @property
    def ovnsb_connection(self):
        return 'unix:' + os.path.join(self.venv, 'ovnsb_db.sock')

    def setup_dbs(self):
        super(OvsOvnVenvFixture, self).setup_dbs()
        self.create_db('ovnsb.db', self.ovnsb_schema)
        self.create_db('ovnnb.db', self.ovnnb_schema)

    def start_ovsdb_processes(self):
        super(OvsOvnVenvFixture, self).start_ovsdb_processes()
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

    def init_processes(self):
        super(OvsOvnVenvFixture, self).init_processes()
        self.call(['ovn-nbctl', 'init'])
        self.call(['ovn-sbctl', 'init'])
        if self.add_chassis:
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
