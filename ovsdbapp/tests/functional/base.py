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
    connection = None
    ovsvenv = venv.OvsVenvFixture(tempfile.mkdtemp(),
                                  ovsdir=os.getenv('OVS_SRCDIR'), remove=True)
    atexit.register(ovsvenv.cleanUp)
    ovsvenv.setUp()

    @classmethod
    def set_connection(cls):
        if cls.connection is not None:
            return
        if cls.schema == "Open_vSwitch":
            conn = cls.ovsvenv.ovs_connection
        elif cls.schema == "OVN_Northbound":
            conn = cls.ovsvenv.ovnnb_connection
        elif cls.schema == "OVN_Southbound":
            conn = cls.ovsvenv.ovnsb_connection
        else:
            raise TypeError("Unknown schema '%s'" % cls.schema)
        idl = connection.OvsdbIdl.from_server(conn, cls.schema)
        cls.connection = connection.Connection(idl, constants.DEFAULT_TIMEOUT)

    def setUp(self):
        super(FunctionalTestCase, self).setUp()
        self.set_connection()
