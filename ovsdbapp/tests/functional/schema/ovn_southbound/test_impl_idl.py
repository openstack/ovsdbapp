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

from ovsdbapp.backend.ovs_idl import event as ovsdb_event
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.ovn_northbound import fixtures as nbfix
from ovsdbapp.tests.functional.schema.ovn_southbound import event
from ovsdbapp.tests.functional.schema.ovn_southbound import fixtures as sbfix
from ovsdbapp.tests import utils

# Keep the class here for backward compatiblity
WaitForPortBindingEvent = event.WaitForPortBindingEvent


class OvnSouthboundTest(base.FunctionalTestCase):
    schemas = ['OVN_Southbound', 'OVN_Northbound']

    def setUp(self):
        super(OvnSouthboundTest, self).setUp()
        self.api = self.useFixture(
            sbfix.SbApiFixture(self.connection['OVN_Southbound'])).obj
        self.nbapi = self.useFixture(
            nbfix.NbApiFixture(self.connection['OVN_Northbound'])).obj
        self.handler = ovsdb_event.RowEventHandler()
        self.api.idl.notify = self.handler.notify

    def _chassis_add(self, encap_types, encap_ip, *args, **kwargs):
        chassis = kwargs.pop('chassis', utils.get_rand_device_name())
        c = self.useFixture(sbfix.ChassisFixture(
            self.api, chassis=chassis, encap_types=encap_types,
            encap_ip=encap_ip, *args, **kwargs)).obj
        self.assertIn(c, self.api.chassis_list().execute(check_error=True))
        self.assertEqual(c.name, chassis)
        self.assertEqual(set(encap_types), {e.type for e in c.encaps})
        self.assertTrue(all(encap_ip == e.ip for e in c.encaps))
        return c

    def test_chassis_add(self):
        self._chassis_add(['vxlan', 'geneve'], '192.0.2.1')

    def test_chassis_add_exists(self):
        chassis = utils.get_rand_device_name()
        self._chassis_add(['vxlan'], '192.0.2.1', chassis=chassis)
        cmd = self.api.chassis_add(chassis, ['vxlan'], '192.0.2.1')
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_chassis_add_may_exist(self):
        chassis = utils.get_rand_device_name()
        self._chassis_add(['vxlan'], '192.0.2.1', chassis=chassis)
        self._chassis_add(['vxlan'], '192.0.2.1', chassis=chassis,
                          may_exist=True)

    def test_chassis_add_columns(self):
        chassis = utils.get_rand_device_name()
        hostname = "testhostname"
        extids = {'my': 'external_id', 'is': 'set'}
        ch = self._chassis_add(['vxlan'], '192.0.2.1', chassis=chassis,
                               hostname=hostname, external_ids=extids)
        self.assertEqual(hostname, ch.hostname)
        self.assertEqual(extids, ch.external_ids)

    def test_chassis_del(self):
        name = utils.get_rand_device_name()
        chassis = self._chassis_add(['vxlan'], '192.0.2.1', chassis=name)
        self.api.chassis_del(chassis.name).execute(check_error=True)
        self.assertNotIn(chassis, self.api.chassis_list().execute())

    def test_chass_del_no_exist(self):
        name = utils.get_rand_device_name()
        cmd = self.api.chassis_del(name)
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_chassis_del_if_exists(self):
        name = utils.get_rand_device_name()
        self.api.chassis_del(name, if_exists=True).execute(check_error=True)

    def _add_chassis_switch_port(self):
        cname, sname, pname = (utils.get_rand_device_name(prefix=p)
                               for p in ("chassis", "switch", "port"))
        chassis = self._chassis_add(['vxlan'], '192.0.2.1', chassis=cname)
        row_event = event.WaitForPortBindingEvent(pname)
        bogus_event = event.ExceptionalMatchFnEvent(pname)
        # We have to wait for ovn-northd to actually create the port binding
        self.handler.watch_event(bogus_event)
        self.handler.watch_event(row_event)
        with self.nbapi.transaction(check_error=True) as txn:
            switch = txn.add(self.nbapi.ls_add(sname))
            port = txn.add(self.nbapi.lsp_add(sname, pname))
        self.assertTrue(row_event.wait())
        self.assertFalse(bogus_event.wait())
        return chassis, switch.result, port.result

    def test_lsp_bind(self):
        chassis, switch, port = self._add_chassis_switch_port()
        self.api.lsp_bind(port.name, chassis.name).execute(check_error=True)
        binding = idlutils.row_by_value(self.api.idl, 'Port_Binding',
                                        'logical_port', port.name)
        self.assertIn(chassis, binding.chassis)
        return chassis, switch, port

    def test_lsp_bind_exists(self):
        chassis, _switch, port = self.test_lsp_bind()
        cmd = self.api.lsp_bind(port.name, chassis.name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lsp_bind_may_exist(self):
        chassis, _switch, port = self.test_lsp_bind()
        other = self._chassis_add(['vxlan'], '192.0.2.2',
                                  chassis=utils.get_rand_device_name())
        self.api.lsp_bind(port.name, other.name, may_exist=True).execute(
            check_error=True)
        binding = idlutils.row_by_value(self.api.idl, 'Port_Binding',
                                        'logical_port', port.name)
        self.assertNotIn(other, binding.chassis)
        self.assertIn(chassis, binding.chassis)

    def test_lsp_unbind(self):
        _chassis, _switch, port = self.test_lsp_bind()
        self.api.lsp_unbind(port.name).execute(check_error=True)
        binding = idlutils.row_by_value(self.api.idl, 'Port_Binding',
                                        'logical_port', port.name)
        self.assertEqual([], binding.chassis)

    def test_lsp_unbind_no_exist(self):
        cmd = self.api.lsp_unbind(utils.get_rand_device_name())
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lsp_unbind_if_exists(self):
        pname = utils.get_rand_device_name()
        self.api.lsp_unbind(pname, if_exists=True).execute(check_error=True)

    def test_event_with_match_fn_and_conditions(self):
        cond_event = event.MatchFnConditionsEvent(
            events=(event.MatchFnConditionsEvent.ROW_UPDATE,),
            table="SB_Global",
            conditions=(("external_ids", "=", {"foo": "bar"}),))
        self.handler.watch_event(cond_event)
        # Test that we match on condition with an Event that has a match_fn()
        cmd = self.api.db_set("SB_Global", ".", external_ids={"foo": "bar"})
        cmd.execute(check_error=True)
        self.assertTrue(cond_event.wait())
        cond_event.event.clear()
        # Test that we don't ignore the condition when match_fn() returns True
        cmd = self.api.db_set("SB_Global", ".", external_ids={"bar": "bar"})
        cmd.execute(check_error=True)
        self.assertFalse(cond_event.wait())
