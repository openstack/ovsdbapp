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

import uuid

from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.backend.ovs_idl import rowview
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
            self.useFixture(fixtures.BridgeFixture(self.api, bridge['name']))
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
        self.assertCountEqual(self.bridges[:2], res)

    def test_db_find_no_exist(self):
        res = self.api.db_find(
            'Bridge', ('name', '=', 'unpossible')).execute(check_error=True)
        self.assertFalse(res)

    def test_db_find_rows(self):
        res = self.api.db_find_rows(
            'Bridge',
            ('datapath_type', '=', 'fake1')).execute(check_error=True)
        self.assertCountEqual(
            self.bridges[:2],
            [{'name': r.name, 'datapath_type': r.datapath_type} for r in res])

    def test_db_list(self):
        res = self.api.db_list(
            'Bridge',
            columns=('name', 'datapath_type')).execute(check_error=True)
        self.assertTrue(all(b in res for b in self.bridges))

    def test_db_list_nested(self):
        with self.api.transaction(check_error=True):
            self.test_db_list()

    def test_db_list_record(self):
        res = self.api.db_list(
            'Bridge', [self.bridges[0]['name']],
            ('name', 'datapath_type')).execute(check_error=True)
        self.assertEqual(self.bridges[0], res[0])

    def test_db_list_record_no_exist(self):
        cmd = self.api.db_list('Bridge', ['unpossible'])
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_db_list_multiple_records_no_exist(self):
        # Check the case where some records are found and some are not. We
        # should still be getting the RowNotFound exception in this case.
        cmd = self.api.db_list('Bridge',
                               [self.bridges[0]['name'], 'unpossible'])
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_db_list_record_if_exsists(self):
        self.api.db_list('Bridge', ['unpossible'])

    def test_db_list_rows(self):
        res = self.api.db_list_rows('Bridge').execute(check_error=True)
        self.assertTrue(
            set(b['name'] for b in self.bridges).issubset(
                set(b.name for b in res)))

    def test_db_create(self):
        _uuid = self.api.db_create(
            'Queue', external_ids={'x': 'x'}).execute(check_error=True)
        self.assertIsInstance(_uuid, uuid.UUID)
        self.api.db_destroy('Queue', _uuid).execute(check_error=True)

    def test_db_create_row(self):
        row = self.api.db_create_row(
            'Queue', external_ids={'x': 'x'}).execute(check_error=True)
        self.assertIsInstance(row, rowview.RowView)
        self.api.db_destroy('Queue', row.uuid).execute(check_error=True)

    def test_db_set_args(self):
        brname = self.bridges[0]['name']
        br = self.api.lookup('Bridge', brname)
        ext_ids = {'test': 'value'}
        self.api.db_set('Bridge', brname,
                        ('external_ids', ext_ids)).execute(check_error=True)
        self.assertEqual(ext_ids, br.external_ids)

    def test_db_set_kwargs(self):
        brname = self.bridges[0]['name']
        br = self.api.lookup('Bridge', brname)
        ext_ids = {'test': 'value'}
        self.api.db_set('Bridge', brname,
                        external_ids=ext_ids).execute(check_error=True)
        self.assertEqual(ext_ids, br.external_ids)

    def test_db_set_if_exists(self):
        brname = self.bridges[0]['name']
        br = self.api.lookup('Bridge', brname)
        ext_ids = {'test': 'value'}
        self.api.db_set('Bridge', brname, if_exists=True,
                        external_ids=ext_ids).execute(check_error=True)
        self.assertEqual(ext_ids, br.external_ids)

    def test_db_set_if_exists_missing(self):
        brname = "missing_bridge"
        ext_ids = {'test': 'value'}
        # Just ensure that this completes without throwing an exception
        self.api.db_set('Bridge', brname, if_exists=True,
                        external_ids=ext_ids).execute(check_error=True)

    def test_db_set_args_and_kwrags(self):
        brname = self.bridges[0]['name']
        br = self.api.lookup('Bridge', brname)
        ext_ids = {'test': 'value'}
        ext_ids2 = {'test2': 'value2'}
        self.api.db_set('Bridge', brname, ('external_ids', ext_ids),
                        external_ids=ext_ids2).execute(check_error=True)
        self.assertEqual(ext_ids, br.external_ids)
