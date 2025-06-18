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
import uuid

from ovsdbapp import api
from ovsdbapp.backend import ovs_idl
from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp import constants
from ovsdbapp.tests.functional import base
from ovsdbapp import venv

# flake8 wrongly reports line 1 in the license has an f-string with a single }
# flake8: noqa E999

class IdlTestApi(ovs_idl.Backend, api.API):
    pass


class IdlUtilsRow2StrTestCase(base.VenvPerClassFunctionalTestCase):

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

    def test_simple_row(self):
        data = {
            "integer_col": 42,
            "real_col": 3.14159,
            "boolean_col": True,
            "string_col": "test_simple",
            "uuid_col": uuid.uuid4()}
        row = self.api.db_create_row("SimpleTypes",
                                     **data).execute(check_error=True)
        self.assertEqual(f"SimpleTypes(uuid={row.uuid}, "
                         f"boolean_col={data['boolean_col']}, "
                         f"integer_col={data['integer_col']}, "
                         f"real_col={data['real_col']}, "
                         f"string_col='{data['string_col']}', "
                         f"uuid_col={data['uuid_col']})",
                         str(row))

    def test_optional_row_unset(self):
        row = self.api.db_create_row("OptionalTypes").execute(check_error=True)
        self.assertEqual(f"OptionalTypes(uuid={row.uuid}, opt_boolean=[], "
                         "opt_integer=[], opt_real=[], opt_string=[], "
                         "opt_uuid=[])", str(row))

    def test_option_row_set(self):
        data = {
            "opt_integer": 42,
            "opt_real": 3.14159,
            "opt_boolean": False,
            "opt_string": "foo",
            "opt_uuid": uuid.uuid4()}
        row = self.api.db_create_row("OptionalTypes", **data).execute(
            check_error=True)
        self.assertEqual(f"OptionalTypes(uuid={row.uuid}, "
                         f"opt_boolean={data['opt_boolean']}, "
                         f"opt_integer={data['opt_integer']}, "
                         f"opt_real={data['opt_real']}, "
                         f"opt_string='{data['opt_string']}', "
                         f"opt_uuid={data['opt_uuid']})", str(row))

    def test_set_types(self):
        data = {
            "integer_set": [1, 2, 3],
            "real_set": [1.1, 2.2, 3.3],
            "boolean_set": [True, False],
            "string_set": ["foo", "bar"],
            "uuid_set": [uuid.uuid4(), uuid.uuid4()]}
        string_set_str = ", ".join(repr(d) for d in sorted(data['string_set']))
        uuid_set_str = ", ".join(str(d) for d in sorted(data['uuid_set']))
        row = self.api.db_create_row("SetTypes", **data).execute()
        self.assertEqual(f"SetTypes(uuid={row.uuid}, "
                         f"boolean_set={sorted(data['boolean_set'])}, "
                         f"integer_set={sorted(data['integer_set'])}, "
                         f"real_set={sorted(data['real_set'])}, "
                         f"string_set=[{string_set_str}], "
                         f"uuid_set=[{uuid_set_str}])", str(row))

    def test_map_types(self):
        import itertools
        vals = {"int": 42, "real": 3.14, "bool": False,
                "string": "foo", "uuid": uuid.uuid4()}
        data = {f"{a}_{b}_map": {vals[a]: vals[b]}
                for a, b in itertools.product(vals.keys(), vals.keys())}
        row = self.api.db_create_row("MapTypes", **data).execute()
        self.assertEqual(
            f"MapTypes(uuid={row.uuid}, "
            f"bool_bool_map={{{vals['bool']}: {vals['bool']}}}, "
            f"bool_int_map={{{vals['bool']}: {vals['int']}}}, "
            f"bool_real_map={{{vals['bool']}: {vals['real']}}}, "
            f"bool_string_map={{{vals['bool']}: '{vals['string']}'}}, "
            f"bool_uuid_map={{{vals['bool']}: {vals['uuid']}}}, "
            f"int_bool_map={{{vals['int']}: {vals['bool']}}}, "
            f"int_int_map={{{vals['int']}: {vals['int']}}}, "
            f"int_real_map={{{vals['int']}: {vals['real']}}}, "
            f"int_string_map={{{vals['int']}: '{vals['string']}'}}, "
            f"int_uuid_map={{{vals['int']}: {vals['uuid']}}}, "
            f"real_bool_map={{{vals['real']}: {vals['bool']}}}, "
            f"real_int_map={{{vals['real']}: {vals['int']}}}, "
            f"real_real_map={{{vals['real']}: {vals['real']}}}, "
            f"real_string_map={{{vals['real']}: '{vals['string']}'}}, "
            f"real_uuid_map={{{vals['real']}: {vals['uuid']}}}, "
            f"string_bool_map={{'{vals['string']}': {vals['bool']}}}, "
            f"string_int_map={{'{vals['string']}': {vals['int']}}}, "
            f"string_real_map={{'{vals['string']}': {vals['real']}}}, "
            f"string_string_map={{'{vals['string']}': '{vals['string']}'}}, "
            f"string_uuid_map={{'{vals['string']}': {vals['uuid']}}}, "
            f"uuid_bool_map={{{vals['uuid']}: {vals['bool']}}}, "
            f"uuid_int_map={{{vals['uuid']}: {vals['int']}}}, "
            f"uuid_real_map={{{vals['uuid']}: {vals['real']}}}, "
            f"uuid_string_map={{{vals['uuid']}: '{vals['string']}'}}, "
            f"uuid_uuid_map={{{vals['uuid']}: {vals['uuid']}}})", str(row))

    def test_ref_types(self):
        ref_target = self.api.db_create_row("RefTarget", value=42).execute()
        ref_target = ref_target.uuid
        data = {
            "single_ref": ref_target,
            "ref_set": [ref_target],
            "ref_map_key": {ref_target: "foo"},
            "ref_map_value": {"foo": ref_target},
            "ref_map_both": {ref_target: ref_target}}
        row = self.api.db_create_row("RefTypes", **data).execute()
        self.assertEqual(
            f"RefTypes(uuid={row.uuid}, "
            "opt_ref=[], "
            f"ref_map_both={{{ref_target}: {ref_target}}}, "
            f"ref_map_key={{{ref_target}: 'foo'}}, "
            f"ref_map_value={{'foo': {ref_target}}}, "
            f"ref_set=[{ref_target}], "
            f"single_ref={ref_target})", str(row))
