# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import atexit
import os
import tempfile

from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp import constants
from ovsdbapp.tests import base
from ovsdbapp import venv


class FunctionalTestCase(base.TestCase):
    _connections = None
    ovsvenv = venv.OvsOvnVenvFixture(tempfile.mkdtemp(),
                                     ovsdir=os.getenv('OVS_SRCDIR'),
                                     remove=not bool(os.getenv('KEEP_VENV')))
    atexit.register(ovsvenv.cleanUp)
    ovsvenv.setUp()
    schema_map = {'Open_vSwitch': ovsvenv.ovs_connection,
                  'OVN_Northbound': ovsvenv.ovnnb_connection,
                  'OVN_Southbound': ovsvenv.ovnsb_connection}
    ovsvenvlog = None
    if os.getenv('KEEP_VENV') and os.getenv('VIRTUAL_ENV'):
        ovsvenvlog = open(os.path.join(os.getenv('VIRTUAL_ENV'),
                                       'ovsvenv.%s' % os.getpid()), 'a+')
        atexit.register(ovsvenvlog.close)
        ovsvenvlog.write("%s\n" % ovsvenv.venv)

    @classmethod
    def venv_log(cls, val):
        if cls.ovsvenvlog:
            cls.ovsvenvlog.write("%s\n" % val)

    @property
    def connection(self):
        if len(self.schemas) == 1:
            return self.__class__._connections[self.schemas[0]]
        return self.__class__._connections

    @classmethod
    def set_connection(cls):
        if cls._connections is not None:
            return
        cls._connections = {}
        for schema in cls.schemas:
            cls._connections[schema] = cls.create_connection(schema)

    @classmethod
    def create_connection(cls, schema):
        idl = connection.OvsdbIdl.from_server(cls.schema_map[schema], schema)
        return connection.Connection(idl, constants.DEFAULT_TIMEOUT)

    def setUp(self):
        super(FunctionalTestCase, self).setUp()
        self.venv_log(self.id())
        self.set_connection()
