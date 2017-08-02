# Copyright (c) 2017 Red Hat Inc.
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

from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.schema.open_vswitch import impl_idl
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.open_vswitch import fixtures
from ovsdbapp.tests import utils


class TestBackendDb(base.FunctionalTestCase):
    schemas = ["Open_vSwitch"]

    def setUp(self):
        self.bridges = [
            {'name': utils.get_rand_device_name(),
             'datapath_type': 'fake1'},
            {'name': utils.get_rand_device_name(),
             'datapath_type': 'fake1'},
            {'name': utils.get_rand_device_name(),
             'datapath_type': 'fake2'}
        ]

        super(TestBackendDb, self).setUp()
        self.api = impl_idl.OvsdbIdl(self.connection)
        for bridge in self.bridges:
            self.useFixture(fixtures.BridgeFixture(bridge['name']))
            for col, val in bridge.items():
                if col == 'name':
                    continue
                self.api.db_set(
                    'Bridge', bridge['name'], (col, val)).execute(
                        check_error=True)

    def test_db_find(self):
        res = self.api.db_find(
            'Bridge',
            ('datapath_type', '=', 'fake1'),
            columns=['name', 'datapath_type']).execute(check_error=True)
        self.assertItemsEqual(self.bridges[:2], res)

    def test_db_find_no_exist(self):
        res = self.api.db_find(
            'Bridge', ('name', '=', 'unpossible')).execute(check_error=True)
        self.assertFalse(res)

    def test_db_find_rows(self):
        res = self.api.db_find_rows(
            'Bridge',
            ('datapath_type', '=', 'fake1')).execute(check_error=True)
        self.assertItemsEqual(
            self.bridges[:2],
            [{'name': r.name, 'datapath_type': r.datapath_type} for r in res])

    def test_db_list(self):
        res = self.api.db_list(
            'Bridge',
            columns=('name', 'datapath_type')).execute(check_error=True)
        self.assertTrue(all(b in res for b in self.bridges))

    def test_db_list_record(self):
        res = self.api.db_list(
            'Bridge', [self.bridges[0]['name']],
            ('name', 'datapath_type')).execute(check_error=True)
        self.assertEqual(self.bridges[0], res[0])

    def test_db_list_record_no_exist(self):
        cmd = self.api.db_list('Bridge', ['unpossible'])
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_db_list_record_if_exsists(self):
        self.api.db_list('Bridge', ['unpossible'])

    def test_db_list_rows(self):
        res = self.api.db_list_rows('Bridge').execute(check_error=True)
        self.assertTrue(
            set(b['name'] for b in self.bridges).issubset(
                set(b.name for b in res)))
