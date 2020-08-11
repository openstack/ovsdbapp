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

import collections
from unittest import mock

from ovsdbapp.backend import ovs_idl
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.tests import base


class FakeRow(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeTable(object):
    rows = collections.UserDict({'fake-id-1':
                                 FakeRow(uuid='fake-id-1', name='Fake1')})
    rows.indexes = {}
    indexes = []


class FakeBackend(ovs_idl.Backend):
    schema = "FakeSchema"
    tables = {'Faketable': FakeTable()}
    lookup_table = {'Faketable': idlutils.RowLookup('Faketable', 'name', None)}

    def start_connection(self, connection):
        pass

    def autocreate_indices(self):
        pass


class TestBackendOvsIdl(base.TestCase):
    def setUp(self):
        super(TestBackendOvsIdl, self).setUp()
        self.backend = FakeBackend(mock.MagicMock())

    def test_lookup_found(self):
        row = self.backend.lookup('Faketable', 'Fake1')
        self.assertEqual('Fake1', row.name)

    def test_lookup_not_found(self):
        self.assertRaises(idlutils.RowNotFound, self.backend.lookup,
                          'Faketable', 'notthere')

    def test_lookup_not_found_default(self):
        row = self.backend.lookup('Faketable', 'notthere', "NOT_FOUND")
        self.assertEqual(row, "NOT_FOUND")
