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

from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.ovn_ic_northbound import fixtures
from ovsdbapp.tests import utils


class OvnIcNorthboundTest(base.FunctionalTestCase):
    schemas = ['OVN_IC_Northbound']
    fixture_class = base.venv.OvsOvnIcVenvFixture

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema_map = cls.schema_map.copy()
        cls.schema_map['OVN_IC_Northbound'] = cls.ovsvenv.ovn_icnb_connection

    def setUp(self):
        if not self.ovsvenv.has_icnb():
            self.skipTest("Installed version of OVN does not support ICNB")
        super(OvnIcNorthboundTest, self).setUp()
        self.api = self.useFixture(
            fixtures.IcNbApiFixture(self.connection)).obj
        self.table = self.api.tables['Transit_Switch']

    def _ts_add(self, *args, **kwargs):
        fix = self.useFixture(fixtures.TransitSwitchesFixture(self.api, *args,
                                                              **kwargs))
        self.assertIn(fix.obj.uuid, self.table.rows)
        return fix.obj

    def _test_ts_get(self, col):
        ts = self._ts_add(switch=utils.get_rand_device_name())
        val = getattr(ts, col)
        found = self.api.ts_get(val).execute(check_error=True)
        self.assertEqual(ts, found)

    def test_ts_get_uuid(self):
        self._test_ts_get('uuid')

    def test_ts_get_name(self):
        self._test_ts_get('name')

    def test_ts_add_name(self):
        name = utils.get_rand_device_name()
        ts = self._ts_add(name)
        self.assertEqual(name, ts.name)

    def test_ts_add_existing(self):
        name = utils.get_rand_device_name()
        self._ts_add(name)
        cmd = self.api.ts_add(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_ts_add_may_exist(self):
        name = utils.get_rand_device_name()
        sw = self._ts_add(name)
        sw2 = self.api.ts_add(name, may_exist=True).execute(check_error=True)
        self.assertEqual(sw, sw2)

    def test_ts_add_columns(self):
        external_ids = {'mykey': 'myvalue', 'yourkey': 'yourvalue'}
        ts = self._ts_add(switch=utils.get_rand_device_name(),
                          external_ids=external_ids)
        self.assertEqual(external_ids, ts.external_ids)

    def test_ts_del(self):
        sw = self._ts_add(switch=utils.get_rand_device_name())
        self.api.ts_del(sw.uuid).execute(check_error=True)
        self.assertNotIn(sw.uuid, self.table.rows)

    def test_ts_del_by_name(self):
        name = utils.get_rand_device_name()
        self._ts_add(name)
        self.api.ts_del(name).execute(check_error=True)
        found = self.api.ts_get(name).execute()
        self.assertIsNone(found)

    def test_ts_del_no_existing(self):
        name = utils.get_rand_device_name()
        cmd = self.api.ts_del(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_ts_del_if_exists(self):
        name = utils.get_rand_device_name()
        self.api.ts_del(name, if_exists=True).execute(check_error=True)
        found = self.api.ts_get(name).execute()
        self.assertIsNone(found)

    def test_ts_list(self):
        switches = {self._ts_add(str(i)) for i in range(3)}
        switch_set = set(self.api.ts_list().execute(check_error=True))
        self.assertTrue(switches.issubset(switch_set))
