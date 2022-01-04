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

from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.ovn_northbound import fixtures
from ovsdbapp.tests import utils


class TestOvnNbIndex(base.FunctionalTestCase):
    schemas = ['OVN_Northbound']

    def setUp(self):
        super(TestOvnNbIndex, self).setUp()
        self.api = self.useFixture(
            fixtures.NbApiFixture(self.connection)).obj

    def test_find(self):
        # This test will easily time out if indexing isn't used
        length = 2000
        basename = utils.get_rand_device_name('testpg')
        with self.api.transaction(check_error=True) as txn:
            for i in range(length):
                txn.add(self.api.pg_add("%s%d" % (basename, i)))
        match = "%s%d" % (basename, length / 2)
        pg = self.api.lookup('Port_Group', match)
        self.assertEqual(pg.name, match)

    def test_default_indices(self):
        self.assertTrue(self.api.lookup_table)
        for key, (table, col, _) in self.api.lookup_table.items():
            idl_table = self.api.tables[table]
            self.assertIn(col, idl_table.rows.indexes)


class TestOvnNbWithoutIndex(base.FunctionalTestCase):
    schemas = ['OVN_Northbound']

    def setUp(self):
        super(TestOvnNbWithoutIndex, self).setUp()
        self.api = self.useFixture(
            fixtures.NbApiFixture(self.connection, start=False,
                                  auto_index=False)).obj

    @mock.patch.object(idlutils, 'table_lookup')
    def test_create_index(self, table_lookup):
        self.assertFalse(self.api.tables['Logical_Switch'].rows.indexes)
        self.api.create_index("Logical_Switch", "name")
        self.api.start_connection(self.connection)
        name = utils.get_rand_device_name("testswitch")
        self.api.ls_add(name).execute(check_error=True)
        sw = self.api.lookup('Logical_Switch', name)
        self.assertEqual(sw.name, name)
        self.assertRaises(idlutils.RowNotFound, self.api.lookup,
                          'Logical_Switch', 'nothere')
        table_lookup.assert_not_called()
