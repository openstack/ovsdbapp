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

import os

from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp import constants
from ovsdbapp.tests import base
from ovsdbapp import venv


class VenvPerClassFunctionalTestCase(base.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.virtualenv = venv.VenvFixture(
            remove=not bool(os.getenv("KEEP_VENV")))
        cls.virtualenv.setUp()
        cls.ovsvenvlog = None
        if os.getenv('KEEP_VENV') and os.getenv('VIRTUAL_ENV'):
            cls.ovsvenvlog = open(
                os.path.join(os.getenv('VIRTUAL_ENV'),
                             'ovsvenv.%s' % os.getpid()), 'a+')
            cls.ovsvenvlog.write("%s\n" % cls.ovsvenv.venv)

    @classmethod
    def tearDownClass(cls):
        if cls.ovsvenvlog:
            cls.ovsvenvlog.close()
        cls.virtualenv.cleanUp()
        super().tearDownClass()

    @classmethod
    def venv_log(cls, val):
        if cls.ovsvenvlog:
            cls.ovsvenvlog.write("%s\n" % val)

    def setUp(self):
        super().setUp()
        self.venv_log(self.id())


class FunctionalTestCase(VenvPerClassFunctionalTestCase):
    _connections = None
    fixture_class = venv.OvsOvnVenvFixture

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.ovsvenv = cls.fixture_class(
            cls.virtualenv,
            ovsdir=os.getenv('OVS_SRCDIR'),
            ovndir=os.getenv('OVN_SRCDIR'))
        cls.ovsvenv.setUp()
        cls.schema_map = {'Open_vSwitch': cls.ovsvenv.ovs_connection,
                          'OVN_Northbound': cls.ovsvenv.ovnnb_connection,
                          'OVN_Southbound': cls.ovsvenv.ovnsb_connection,
                          }

    @classmethod
    def tearDownClass(cls):
        cls.ovsvenv.cleanUp()
        super().tearDownClass()

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
        super().setUp()
        self.set_connection()
