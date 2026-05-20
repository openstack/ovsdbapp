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

import itertools
import os
import uuid

from ovsdbapp import api
from ovsdbapp.backend import ovs_idl
from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp import constants
from ovsdbapp.tests.functional import base
from ovsdbapp import venv


class IdlTestApi(ovs_idl.Backend, api.API):
    pass


class TestRowViewAsDict(base.VenvPerClassFunctionalTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        schema_path = os.path.join(
            os.path.dirname(__file__), 'idltest.ovsschema')
        cls.ovsdb_server = venv.OvsdbServerFixture(
            cls.virtualenv, "idltest", schema_path)
        cls.ovsdb_server.setUp()

    @classmethod
    def tearDownClass(cls):
        cls.ovsdb_server.cleanUp()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        self.idl = connection.OvsdbIdl.from_server(
            self.ovsdb_server.connection, "idltest")
        self.connection = connection.Connection(self.idl,
                                                constants.DEFAULT_TIMEOUT)
        self.api = IdlTestApi(self.connection)

    def test_simple_types(self):
        data = {
            "integer_col": 42,
            "real_col": 3.14159,
            "boolean_col": True,
            "string_col": "test_simple",
            "uuid_col": uuid.uuid4()}
        row = self.api.db_create_row("SimpleTypes",
                                     **data).execute(check_error=True)
        result = row.asdict()
        self.assertEqual(row.uuid, result['_uuid'])
        for col, val in data.items():
            self.assertEqual(val, result[col])

    def test_optional_types_unset(self):
        row = self.api.db_create_row("OptionalTypes").execute(check_error=True)
        result = row.asdict()
        self.assertEqual(row.uuid, result['_uuid'])
        for col in ("opt_integer", "opt_real", "opt_boolean",
                    "opt_string", "opt_uuid"):
            self.assertEqual([], result[col])

    def test_optional_types_set(self):
        data = {
            "opt_integer": 42,
            "opt_real": 3.14159,
            "opt_boolean": False,
            "opt_string": "foo",
            "opt_uuid": uuid.uuid4()}
        row = self.api.db_create_row("OptionalTypes", **data).execute(
            check_error=True)
        result = row.asdict()
        self.assertEqual(row.uuid, result['_uuid'])
        for col, val in data.items():
            self.assertEqual(val, result[col])

    def test_set_types(self):
        data = {
            "integer_set": [1, 2, 3],
            "real_set": [1.1, 2.2, 3.3],
            "boolean_set": [True, False],
            "string_set": ["foo", "bar"],
            "uuid_set": [uuid.uuid4(), uuid.uuid4()]}
        row = self.api.db_create_row("SetTypes", **data).execute(
            check_error=True)
        result = row.asdict()
        self.assertEqual(row.uuid, result['_uuid'])
        for col, val in data.items():
            self.assertEqual(sorted(val), sorted(result[col]))

    def test_map_types(self):
        vals = {"int": 42, "real": 3.14, "bool": False,
                "string": "foo", "uuid": uuid.uuid4()}
        data = {f"{a}_{b}_map": {vals[a]: vals[b]}
                for a, b in itertools.product(vals.keys(), vals.keys())}
        row = self.api.db_create_row("MapTypes", **data).execute(
            check_error=True)
        result = row.asdict()
        self.assertEqual(row.uuid, result['_uuid'])
        for col, val in data.items():
            self.assertEqual(val, result[col])

    def test_ref_types(self):
        ref_target = self.api.db_create_row("RefTarget", value=42).execute(
            check_error=True)
        ref_uuid = ref_target.uuid
        data = {
            "single_ref": ref_uuid,
            "ref_set": [ref_uuid],
            "ref_map_key": {ref_uuid: "foo"},
            "ref_map_value": {"foo": ref_uuid},
            "ref_map_both": {ref_uuid: ref_uuid}}
        row = self.api.db_create_row("RefTypes", **data).execute(
            check_error=True)
        result = row.asdict()
        self.assertEqual(row.uuid, result['_uuid'])
        self.assertEqual(ref_uuid, result['single_ref'])
        self.assertEqual([], result['opt_ref'])
        self.assertEqual([ref_uuid], result['ref_set'])
        self.assertEqual({ref_uuid: "foo"}, result['ref_map_key'])
        self.assertEqual({"foo": ref_uuid}, result['ref_map_value'])
        self.assertEqual({ref_uuid: ref_uuid}, result['ref_map_both'])

    def test_asdict_is_a_copy(self):
        data = {"string_col": "original", "integer_col": 1,
                "real_col": 1.0, "boolean_col": True,
                "uuid_col": uuid.uuid4()}
        row = self.api.db_create_row("SimpleTypes",
                                     **data).execute(check_error=True)
        result = row.asdict()
        result['string_col'] = "modified"
        result['integer_col'] = 999
        self.assertEqual("original", row.string_col)
        self.assertEqual(1, row.integer_col)
