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

import errno
import json
import os
import shutil
import signal
import subprocess
import tempfile

import fixtures

# These are the valid dummy values for ovs-vswitchd process. They are here just
# to get user enumeration. See man ovs-vswitchd(8) for more information.
DUMMY_OVERRIDE_ALL = 'override'
DUMMY_OVERRIDE_SYSTEM = 'system'
DUMMY_OVERRIDE_NONE = ''


def get_pid(filename):
    with open(filename) as f:
        return int(f.read().strip())


def kill_pid(pidfile):
    """Kill process using PID from pidfile, handling common exceptions."""
    try:
        os.kill(get_pid(pidfile), signal.SIGTERM)
    except (FileNotFoundError, ProcessLookupError):
        pass  # Process already stopped or pidfile doesn't exist


class VenvFixture(fixtures.Fixture):

    def __init__(self, venvdir=None, remove=False):
        super().__init__()
        self.venvdir = venvdir or tempfile.mkdtemp()
        self.env = {
            "OVS_RUNDIR": self.venvdir,
            "OVS_LOGDIR": self.venvdir,
            "OVS_DBDIR": self.venvdir,
            "OVS_SYSCONFDIR": self.venvdir,
            "PATH": ""}
        self.remove = remove

    def deactivate(self):
        if self.remove:
            shutil.rmtree(self.venvdir, ignore_errors=True)

    def _setUp(self):
        super()._setUp()
        self.addCleanup(self.deactivate)

    def call(self, cmd, *args, **kwargs):
        cwd = kwargs.pop("cwd", self.venvdir)
        try:
            return subprocess.check_output(
                cmd, *args, env=self.env, stderr=subprocess.STDOUT,
                cwd=cwd, **kwargs)
        except subprocess.CalledProcessError as e:
            raise Exception(
                f"Command {cmd} failed with error: {e.returncode} {e.output}")

    def set_path(self, path):
        self.env["PATH"] = path

    def prepend_paths(self, *dirs):
        self.env["PATH"] = os.pathsep.join((os.pathsep.join(dirs),
                                            self.env["PATH"]))

    @classmethod
    def from_fixtures(cls, fixtures, *args, **kwargs):
        """Create a VenvFixture with an environment from a list of fixtures."""
        venv = cls(*args, **kwargs)
        for fixture in fixtures:
            # use dict.fromkeys to deduplicate paths while maintaining order
            existing_path = dict.fromkeys(
                fixture.env["PATH"].split(os.pathsep))
            new_path = dict.fromkeys(fixture.env["PATH"].split(os.pathsep))
            new_path.update(existing_path)
            venv.env.update(fixture.env)
            venv.env["PATH"] = os.pathsep.join(new_path)
        return venv


class OvsdbServerFixture(fixtures.Fixture):
    def __init__(self, venv, name, schema_filename, ovsdir=None, *args):
        super().__init__()
        self.venv = venv
        self.name = name
        self.schema_filename = schema_filename
        self.ovsdir = ovsdir
        self.args = args
        self.additional_dbs = []
        self.schema = self.get_schema_name()
        if not self.ovsdir:
            self.venv.set_path(os.getenv("PATH"))
        elif not os.path.isdir(self.ovsdir):
            raise FileNotFoundError(
                errno.ENOENT, "OVS source directory not found", self.ovsdir)
        else:
            self.venv.prepend_paths(os.path.join(self.ovsdir, "ovsdb"),
                                    os.path.join(self.ovsdir, "utilities"))

    def get_schema_name(self):
        with open(self.schema_filename) as f:
            return json.load(f)["name"]

    @property
    def unix_socket(self):
        return os.path.join(self.venv.venvdir, f"{self.name}.sock")

    @property
    def pidfile(self):
        return os.path.join(self.venv.venvdir, f"{self.name}.pid")

    @property
    def logfile(self):
        return os.path.join(self.venv.venvdir, f"{self.name}.log")

    @property
    def dbfile(self):
        return os.path.join(self.venv.venvdir, f"{self.name}.db")

    @property
    def connection(self):
        return "unix:" + self.unix_socket

    def deactivate(self):
        kill_pid(self.pidfile)

    def create_db(self, dbfile=None, schema_filename=None):
        dbfile = dbfile or self.dbfile
        schema_filename = schema_filename or self.schema_filename
        if os.path.isfile(dbfile):
            return
        self.venv.call(["ovsdb-tool", "-v", "create", dbfile, schema_filename])

    def start(self):
        base_args = (
            "ovsdb-server",
            "--detach",
            "--no-chdir",
            "-vconsole:off",
            f"--pidfile={self.pidfile}",
            f"--log-file={self.logfile}",
            f"--remote=p{self.connection}")
        # TODO(twilson) Make SSL configurable since not all schemas
        # will support it the same way, e.g. OVN supports this but OVS doesn't
        #   f"--private-key=db:{self.schema},SSL,private_key",
        #   f"--certificate=db:{self.schema},SSL,certificate",
        #   f"--ca-cert=db:{self.schema},SSL,ca_cert")

        dbs = (self.dbfile,) + tuple(self.additional_dbs)
        self.venv.call(base_args + self.args + dbs)

    def _setUp(self):
        super()._setUp()
        self.addCleanup(self.deactivate)
        self.create_db()
        self.start()


class VswitchdFixture(fixtures.Fixture):
    def __init__(self, venv, ovsdb_server, dummy=DUMMY_OVERRIDE_ALL):
        super().__init__()
        self.venv = venv
        self.ovsdb_server = ovsdb_server
        self.dummy = dummy

    @property
    def pidfile(self):
        return os.path.join(self.venv.venvdir, "ovs-vswitchd.pid")

    @property
    def logfile(self):
        return os.path.join(self.venv.venvdir, "ovs-vswitchd.log")

    @property
    def dummy_arg(self):
        return "--enable-dummy=%s" % self.dummy

    def deactivate(self):
        kill_pid(self.pidfile)

    def start(self):
        self.venv.call(
            ["ovs-vswitchd",
             "--detach",
             "--no-chdir",
             f"--pidfile={self.pidfile}",
             "-vconsole:off",
             "-vvconn",
             "-vnetdev_dummy",
             f"--log-file={self.logfile}",
             self.dummy_arg,
             self.ovsdb_server.connection])

    def _setUp(self):
        super()._setUp()
        self.addCleanup(self.deactivate)
        self.start()


class NorthdFixture(fixtures.Fixture):
    def __init__(self, venv, ovnnb_connection, ovnsb_connection):
        super().__init__()
        self.venv = venv
        self.ovnnb_connection = ovnnb_connection
        self.ovnsb_connection = ovnsb_connection

    @property
    def pidfile(self):
        return os.path.join(self.venv.venvdir, "ovn-northd.pid")

    @property
    def logfile(self):
        return os.path.join(self.venv.venvdir, "ovn-northd.log")

    def deactivate(self):
        kill_pid(self.pidfile)

    def start(self):
        self.venv.call([
            "ovn-northd",
            "--detach",
            "--no-chdir",
            f"--pidfile={self.pidfile}",
            "-vconsole:off",
            f"--log-file={self.logfile}",
            f"--ovnsb-db={self.ovnsb_connection}",
            f"--ovnnb-db={self.ovnnb_connection}"])

    def _setUp(self):
        super()._setUp()
        self.addCleanup(self.deactivate)
        self.start()


class OvnControllerFixture(fixtures.Fixture):
    def __init__(self, venv):
        super().__init__()
        self.venv = venv

    @property
    def pidfile(self):
        return os.path.join(self.venv.venvdir, "ovn-controller.pid")

    @property
    def logfile(self):
        return os.path.join(self.venv.venvdir, "ovn-controller.log")

    def deactivate(self):
        kill_pid(self.pidfile)

    def start(self):
        self.venv.call([
            "ovn-controller",
            "--detach",
            "--no-chdir",
            f"--pidfile={self.pidfile}",
            "-vconsole:off",
            f"--log-file={self.logfile}"])

    def _setUp(self):
        super()._setUp()
        self.addCleanup(self.deactivate)
        self.start()


class OvsVenvFixture(fixtures.Fixture):
    OVS_PATHS = (
        os.path.join(os.path.sep, 'usr', 'local', 'share', 'openvswitch'),
        os.path.join(os.path.sep, 'usr', 'share', 'openvswitch'))

    OVS_SCHEMA = 'vswitch.ovsschema'

    def __init__(self, venv=None, ovsdir=None, dummy=DUMMY_OVERRIDE_ALL):
        """Initialize fixture

        :param venv: A VenvFixture
        :param ovsdir: Path to directory containing ovs source codes.
        :param dummy: One of following: an empty string, 'override' or
                      'system'.
        """
        super().__init__()
        self.venv = venv or self.useFixture(VenvFixture())
        self.ovsdir = ovsdir or ()
        self.dummy = dummy
        if ovsdir and os.path.isdir(ovsdir):
            # From source directory
            self.venv.prepend_paths(os.path.join(ovsdir, "utilities"))
            self.venv.prepend_paths(os.path.join(ovsdir, "vswitchd"))
        # NOTE(twilson): We don't use useFixture here because we need to
        # separate the setUp/start and the initialization of the fixture so
        # that we can add additional DBs to the ovsdb-server fixture.
        self.ovsdb_server = OvsdbServerFixture(
            self.venv, "db", self.ovs_schema, self.ovsdir)

    @staticmethod
    def schema_path(search_paths, filename):
        paths = (os.path.join(p, filename) for p in search_paths)
        try:
            return next(p for p in paths if os.path.isfile(p))
        except StopIteration:
            raise FileNotFoundError(
                errno.ENOENT,
                f"Schema file {filename} not found in {search_paths}",
                filename)

    @property
    def ovs_connection(self):
        return self.ovsdb_server.connection

    @property
    def ovs_schema(self):
        return self.schema_path((self.ovsdir,) + self.OVS_PATHS,
                                self.OVS_SCHEMA)

    def call(self, *args, **kwargs):
        # For backwards compatibility
        return self.venv.call(*args, **kwargs)

    def _setUp(self):
        super()._setUp()
        self.useFixture(self.ovsdb_server)
        self.vswitchd = self.useFixture(VswitchdFixture(
            self.venv, self.ovsdb_server, self.dummy))
        self.init_processes()

    def init_processes(self):
        self.venv.call([
            "ovs-vsctl",
            "--no-wait",
            f"--db={self.ovsdb_server.connection}",
            "--",
            "init"])


class OvsOvnVenvFixture(OvsVenvFixture):
    OVN_PATHS = (
        os.path.join(os.path.sep, 'usr', 'local', 'share', 'ovn'),
        os.path.join(os.path.sep, 'usr', 'share', 'ovn')) + (
            OvsVenvFixture.OVS_PATHS)

    NBSCHEMA = 'ovn-nb.ovsschema'
    SBSCHEMA = 'ovn-sb.ovsschema'
    IC_NBSCHEMA = 'ovn-ic-nb.ovsschema'

    def __init__(self, venv, ovndir=None, add_chassis=False, **kwargs):
        super().__init__(venv, **kwargs)
        self.add_chassis = add_chassis
        self.ovndir = ovndir or ()

    @property
    def ovnsb_schema(self):
        search_paths = (self.ovndir,) + self.OVN_PATHS
        return self.schema_path(search_paths, self.SBSCHEMA)

    @property
    def ovnnb_schema(self):
        search_paths = (self.ovndir,) + self.OVN_PATHS
        return self.schema_path(search_paths, self.NBSCHEMA)

    @property
    def ovnnb_connection(self):
        return self.ovnnb_server.connection

    @property
    def ovnsb_connection(self):
        return self.ovnsb_server.connection

    def _setUp(self):
        if self.ovndir and os.path.isdir(self.ovndir):
            # Use OVN source dir - add paths to venv
            self.venv.prepend_paths(
                os.path.join(self.ovndir, "controller"),
                os.path.join(self.ovndir, "northd"),
                os.path.join(self.ovndir, "utilities"))
        self.venv.env.update({"OVN_RUNDIR": self.venv.venvdir})

        self.ovnnb_server = self.useFixture(OvsdbServerFixture(
            self.venv, "ovnnb_db", self.ovnnb_schema, self.ovsdir,
            "--remote=db:OVN_Northbound,NB_Global,connections",
            "--ssl-protocols=db:OVN_Northbound,SSL,ssl_protocols",
            "--ssl-ciphers=db:OVN_Northbound,SSL,ssl_ciphers"))

        self.ovnsb_server = self.useFixture(OvsdbServerFixture(
            self.venv, "ovnsb_db", self.ovnsb_schema, self.ovsdir,
            "--remote=db:OVN_Southbound,SB_Global,connections",
            "--ssl-protocols=db:OVN_Southbound,SSL,ssl_protocols",
            "--ssl-ciphers=db:OVN_Southbound,SSL,ssl_ciphers"))

        self.northd = self.useFixture(NorthdFixture(
            self.venv, self.ovnnb_connection, self.ovnsb_connection))

        self.controller = self.useFixture(OvnControllerFixture(self.venv))
        super()._setUp()

    def init_processes(self):
        super().init_processes()
        self.venv.call(["ovn-nbctl", "init"])
        self.venv.call(["ovn-sbctl", "init"])
        if self.add_chassis:
            self.venv.call([
                "ovs-vsctl", f"--db={self.ovsdb_server.connection}",
                "set", "open", ".",
                "external_ids:system-id=56b18105-5706-46ef-80c4-ff20979ab068",
                "external_ids:hostname=sandbox",
                "external_ids:ovn-encap-type=geneve",
                "external_ids:ovn-encap-ip=127.0.0.1"])
        # TODO(twilson) SSL stuff
        self.venv.call([
            "ovs-vsctl", f"--db={self.ovsdb_server.connection}",
            "set", "open", ".",
            "external_ids:ovn-remote=" + self.ovnsb_connection])


class OvsOvnIcVenvFixture(OvsOvnVenvFixture):

    def _setUp(self):
        if not self.has_icnb():
            return
        self.ovn_icnb_server = self.useFixture(OvsdbServerFixture(
            self.venv, "ovn_ic_nb_db", self.ovn_icnb_schema, self.ovsdir,
            "--remote=db:OVN_IC_Northbound,IC_NB_Global,connections",
            "--ssl-protocols=db:OVN_IC_Northbound,SSL,ssl_protocols",
            "--ssl-ciphers=db:OVN_IC_Northbound,SSL,ssl_ciphers"))
        super()._setUp()

    @property
    def ovn_icnb_connection(self):
        return self.ovn_icnb_server.connection

    @property
    def ovn_icnb_schema(self):
        search_paths = (self.ovndir,) + self.OVN_PATHS
        return self.schema_path(search_paths, self.IC_NBSCHEMA)

    def has_icnb(self):
        return os.path.isfile(self.ovn_icnb_schema)

    def init_processes(self):
        super().init_processes()
        self.venv.call(["ovn-ic-nbctl", "init"])


class OvsVtepVenvFixture(OvsOvnVenvFixture):
    VTEP_SCHEMA = 'vtep.ovsschema'
    VTEP_DB = 'vtep.db'

    def __init__(self, venv, **kwargs):
        super().__init__(venv, **kwargs)
        vtepdir = os.getenv('VTEP_SRCDIR') or ()
        if vtepdir and os.path.isdir(vtepdir):
            # Add VTEP source dir to venv paths
            self.venv.prepend_paths(vtepdir)
        self.vtepdir = vtepdir
        # Uses the existing OVS ovsdb-server fixture and passes in a second db
        self.ovsdb_server.create_db(self.VTEP_DB, self.vtep_schema)
        self.ovsdb_server.additional_dbs.append(self.VTEP_DB)

    def _setUp(self):
        if self.has_vtep:
            super()._setUp()

    @property
    def vtep_schema(self):
        search_paths = (self.vtepdir,) + self.OVS_PATHS
        return self.schema_path(search_paths, self.VTEP_SCHEMA)

    @property
    def has_vtep(self):
        return os.path.isfile(self.vtep_schema)

    def init_processes(self):
        super().init_processes()
        # there are no 'init' method in vtep-ctl,
        # but record in 'Global' table is needed
        self.venv.call(["vtep-ctl",
                        f"--db={self.ovsdb_server.connection}",
                        "show"])
