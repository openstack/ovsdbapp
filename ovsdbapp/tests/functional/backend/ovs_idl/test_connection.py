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

from unittest import mock

from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import constants
from ovsdbapp.tests.functional import base


def create_schema_helper(sh):
    return sh


class TestingOvsIdl(connection.OvsdbIdl):
    schema = 'Open_vSwitch'

    @classmethod
    def from_server(cls, schema_map, tables):
        """Create the Idl instance by pulling the schema from OVSDB server"""
        connection_string = schema_map[cls.schema]
        helper = idlutils.get_schema_helper(connection_string, cls.schema)
        for table in tables:
            helper.register_table(table)
        return cls(connection_string, helper)


class TestOvsdbIdl(base.FunctionalTestCase):
    default_tables = ["Open_vSwitch", "Bridge"]

    def setUp(self):
        super().setUp()
        self.schema = self.get_schema()

    @property
    def idl(self):
        return self._connection.idl

    @classmethod
    def set_connection(cls):
        idl = TestingOvsIdl.from_server(cls.schema_map, cls.default_tables)
        cls._connection = connection.Connection(idl, constants.DEFAULT_TIMEOUT)

    def get_schema(self):
        with mock.patch.object(
                idlutils, 'create_schema_helper',
                side_effect=create_schema_helper):
            return idlutils.get_schema_helper(
                self.schema_map[TestingOvsIdl.schema], TestingOvsIdl.schema)

    def validate_tables(self, tables, present):
        valid_func = self.assertIn if present else self.assertNotIn

        for table in tables:
            valid_func(table, self.idl.tables)

    def test_add_new_table(self):
        tables = ["Port", "Interface"]

        self.validate_tables(tables, present=False)
        self.idl.update_tables(tables, self.schema)
        self.validate_tables(tables, present=True)

    def test_remove_table(self):
        removed_table = "Open_vSwitch"
        new_tables = self.default_tables[:]
        new_tables.remove(removed_table)

        self.validate_tables([removed_table], present=True)

        del self.schema["tables"][removed_table]
        self.idl.update_tables(self.default_tables, self.schema)

        self.validate_tables(new_tables, present=True)
        self.validate_tables([removed_table], present=False)
