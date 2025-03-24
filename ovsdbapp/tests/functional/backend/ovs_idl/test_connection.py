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
from ovsdbapp.backend.ovs_idl import event
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import constants
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.ovn_northbound import test_impl_idl


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

        # ensure that the Idl still works after we update the tables
        self.idl.run()

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


class LsCreateWaitEvent(event.WaitEvent):
    ONETIME = True

    def __init__(self, lsp_name, timeout):
        super().__init__((self.ROW_CREATE,), "Logical_Switch",
                         (("name", "=", lsp_name),), timeout=timeout)


class TestConnectionReconnect(test_impl_idl.OvnNorthboundTest):

    def setUp(self):
        super().setUp()
        # seed the db with an LS prior to setting up notifications
        self.ls = self._ls_add("test")
        self.handler = event.RowEventHandler()
        self.api.idl.notify = self.handler.notify

    def _ls_add(self, name):
        self.api.ls_add(name).execute(check_error=True)
        ls = self.api.ls_get(name).execute(check_error=True)
        self.assertEqual(name, ls.name)
        return ls

    def _create_and_watch_wait_event(self, name):
        event = LsCreateWaitEvent(name, timeout=10)
        self.handler.watch_event(event)
        return event

    def test_force_reconnect(self):
        event = self._create_and_watch_wait_event(self.ls.name)
        self.api.ovsdb_connection.force_reconnect()
        self.assertTrue(event.wait())
        # test things work after reconnect
        event = self._create_and_watch_wait_event("test2")
        self._ls_add("test2")
        self.assertTrue(event.wait())
