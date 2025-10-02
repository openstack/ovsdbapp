# -*- coding: utf-8 -*-
# Copyright (c) 2017 Red Hat, Inc.

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

from ovsdbapp.backend.ovs_idl import event
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.schema.hardware_vtep.commands import get_global_record
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.hardware_vtep import fixtures
from ovsdbapp.tests import utils


class HardwareVtepTest(base.FunctionalTestCase):
    schemas = ["hardware_vtep"]
    fixture_class = base.venv.OvsVtepVenvFixture

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema_map = cls.schema_map.copy()
        cls.schema_map['hardware_vtep'] = cls.ovsvenv.ovs_connection

    def setUp(self):
        if not self.ovsvenv.has_vtep:
            self.skipTest("Installed version of OVS does not support VTEP")
        super().setUp()
        self.api = self.useFixture(
            fixtures.HwVtepApiFixture(self.connection)).obj


class TestPhysicalSwitchOps(HardwareVtepTest):
    def setUp(self):
        super().setUp()
        self.table = self.api.tables['Physical_Switch']
        self.config = get_global_record(self.api)

    def _add_ps(self, *args, **kwargs):
        ps = self.useFixture(fixtures.PhysicalSwitchFixture(self.api, *args,
                                                            **kwargs)).obj
        self.assertIn(ps.uuid, self.table.rows)
        self.assertIn(ps, self.config.switches)
        return ps

    def _test_get_ps(self, col):
        ps = self._add_ps(pswitch=utils.get_rand_device_name())
        val = getattr(ps, col)
        found = self.api.get_ps(val).execute(check_error=True)
        self.assertEqual(ps, found)

    def test_get_ps_uuid(self):
        self._test_get_ps('uuid')

    def test_get_ps_name(self):
        self._test_get_ps('name')

    def test_add_ps_name(self):
        name = utils.get_rand_device_name()
        sw = self._add_ps(name)
        self.assertEqual(name, sw.name)

    def test_add_ps_exists(self):
        name = utils.get_rand_device_name()
        self._add_ps(name)
        cmd = self.api.add_ps(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_add_ps_may_exist(self):
        name = utils.get_rand_device_name()
        sw = self._add_ps(name)
        sw2 = self.api.add_ps(name, may_exist=True).execute(check_error=True)
        self.assertEqual(sw, sw2)

    def test_del_ps(self):
        name = utils.get_rand_device_name()
        sw = self._add_ps(name)
        self.api.del_ps(sw.uuid).execute(check_error=True)
        self.assertNotIn(sw.uuid, self.table.rows)
        self.assertNotIn(sw, self.config.switches)

    def test_del_ps_by_name(self):
        name = utils.get_rand_device_name()
        sw = self._add_ps(name)
        self.api.del_ps(name).execute(check_error=True)
        self.assertNotIn(sw.uuid, self.table.rows)
        self.assertNotIn(sw, self.config.switches)

    def test_del_ps_no_exist(self):
        name = utils.get_rand_device_name()
        cmd = self.api.del_ps(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_del_ps_if_exists(self):
        name = utils.get_rand_device_name()
        self.api.del_ps(name, if_exists=True).execute(check_error=True)

    def test_list_ps(self):
        switches = {self._add_ps(str(i)) for i in range(3)}
        switch_set = set(self.api.list_ps().execute(check_error=True))
        self.assertTrue(switches.issubset(switch_set))


class TestPhysicalPortOps(HardwareVtepTest):
    def setUp(self):
        super().setUp()
        self.table = self.api.tables['Physical_Port']
        self.ps = self.useFixture(fixtures.PhysicalSwitchFixture(
            self.api, utils.get_rand_device_name())).obj

    def _add_port(self, *args, name=None, **kwargs):
        port = self.api.add_port(self.ps.uuid,
                                 name or utils.get_rand_device_name(),
                                 *args, **kwargs).execute()
        self.assertIn(port.uuid, self.table.rows)
        self.assertIn(port, self.ps.ports)
        return port

    def _test_get_port(self, col):
        port = self._add_port(name=utils.get_rand_device_name())
        val = getattr(port, col)
        found = self.api.get_port(val).execute(check_error=True)
        self.assertEqual(port, found)

    def test_get_port_uuid(self):
        self._test_get_port('uuid')

    def test_get_port_name(self):
        self._test_get_port('name')

    def test_add_port_name(self):
        name = utils.get_rand_device_name()
        port = self._add_port(name=name)
        self.assertEqual(name, port.name)

    def test_add_port_exists(self):
        name = utils.get_rand_device_name()
        self._add_port(name=name)
        cmd = self.api.add_port(self.ps.uuid, name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_add_port_may_exist(self):
        name = utils.get_rand_device_name()
        port = self._add_port(name=name)
        port2 = self.api.add_port(
            self.ps.uuid, name, may_exist=True).execute(check_error=True)
        self.assertEqual(port, port2)

    def test_del_port(self):
        port = self._add_port()
        self.api.del_port(self.ps.name, port.name).execute(check_error=True)
        self.assertNotIn(port.uuid, self.table.rows)
        self.assertNotIn(port, self.ps.ports)

    def test_del_port_by_name(self):
        name = utils.get_rand_device_name()
        port = self._add_port(name=name)
        self.api.del_port(self.ps.uuid, name).execute(check_error=True)
        self.assertNotIn(port.uuid, self.table.rows)
        self.assertNotIn(port, self.ps.ports)

    def test_del_port_no_exist(self):
        name = utils.get_rand_device_name()
        cmd = self.api.del_port(self.ps.uuid, name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_del_ps_if_exists(self):
        name = utils.get_rand_device_name()
        self.api.del_port(self.ps.uuid, name, if_exists=True).execute(
            check_error=True)

    def test_list_ps(self):
        ports = {self._add_port(str(i)) for i in range(3)}
        port_set = set(self.api.list_ports(
            self.ps.uuid).execute(check_error=True))
        self.assertTrue(ports.issubset(port_set))


class TestLogicalSwitchOps(HardwareVtepTest):
    def setUp(self):
        super().setUp()
        self.table = self.api.tables['Logical_Switch']
        self.vlan = 10

    def _add_ls(self, *args, **kwargs):
        ls = self.useFixture(fixtures.LogicalSwitchFixture(self.api, *args,
                                                           **kwargs)).obj
        self.assertIn(ls.uuid, self.table.rows)
        return ls

    def _test_get_ls(self, col):
        ls = self._add_ls(switch=utils.get_rand_device_name())
        val = getattr(ls, col)
        found = self.api.get_ls(val).execute(check_error=True)
        self.assertEqual(ls, found)

    def test_get_ls_uuid(self):
        self._test_get_ls('uuid')

    def test_get_ls_name(self):
        self._test_get_ls('name')

    def test_add_ls_name(self):
        name = utils.get_rand_device_name()
        sw = self._add_ls(name)
        self.assertEqual(name, sw.name)

    def test_add_ls_exists(self):
        name = utils.get_rand_device_name()
        self._add_ls(name)
        cmd = self.api.add_ls(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_add_ls_may_exist(self):
        name = utils.get_rand_device_name()
        sw = self._add_ls(name)
        sw2 = self.api.add_ls(name, may_exist=True).execute(check_error=True)
        self.assertEqual(sw, sw2)

    def test_del_ls(self):
        name = utils.get_rand_device_name()
        sw = self._add_ls(name)
        self.api.del_ls(sw.uuid).execute(check_error=True)
        self.assertNotIn(sw.uuid, self.table.rows)

    def test_del_ls_by_name(self):
        name = utils.get_rand_device_name()
        self._add_ls(name)
        self.api.del_ls(name).execute(check_error=True)

    def test_del_ls_no_exist(self):
        name = utils.get_rand_device_name()
        cmd = self.api.del_ls(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_del_ls_if_exists(self):
        name = utils.get_rand_device_name()
        self.api.del_ls(name, if_exists=True).execute(check_error=True)

    def test_list_ls(self):
        switches = {self._add_ls(str(i)) for i in range(3)}
        switch_set = set(self.api.list_ls().execute(check_error=True))
        self.assertTrue(switches.issubset(switch_set))

    def test_bind_unbind_ls(self):
        name = utils.get_rand_device_name()
        switch = self._add_ls(name)
        ps = self.useFixture(fixtures.PhysicalSwitchFixture(
            self.api, name)).obj
        port = self.api.add_port(ps.uuid, name,
                                 may_exist=True).execute(check_error=True)

        self.api.bind_ls(ps.name, port.name,
                         self.vlan, switch.name).execute(check_error=True)
        self.assertEqual(port.vlan_bindings, {self.vlan: switch})

        self.api.unbind_ls(ps.name, port.name,
                           self.vlan).execute(check_error=True)
        self.assertEqual(port.vlan_bindings, {})

    def _test_bind_ls_no_exist(self,
                               pswitch_name=None,
                               port_name=None,
                               switch_name=None):
        cmd = self.api.bind_ls(pswitch_name, port_name, self.vlan, switch_name)
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_bind_ls_pswitch_no_exist(self):
        name = utils.get_rand_device_name()
        switch = self._add_ls(utils.get_rand_device_name())
        self._test_bind_ls_no_exist(pswitch_name=name,
                                    port_name=name,
                                    switch_name=switch.name)

    def test_bind_ls_port_no_exist(self):
        name = utils.get_rand_device_name()
        switch = self._add_ls(utils.get_rand_device_name())
        ps = self.useFixture(fixtures.PhysicalSwitchFixture(
            self.api, utils.get_rand_device_name())).obj
        self._test_bind_ls_no_exist(pswitch_name=ps.name,
                                    port_name=name,
                                    switch_name=switch.name)

    def test_bind_ls_switch_no_exist(self):
        name = utils.get_rand_device_name()
        ps = self.useFixture(fixtures.PhysicalSwitchFixture(
            self.api, utils.get_rand_device_name())).obj
        port = self.api.add_port(ps.uuid, utils.get_rand_device_name(),
                                 may_exist=True).execute(check_error=True)
        self._test_bind_ls_no_exist(pswitch_name=ps.name,
                                    port_name=port.name,
                                    switch_name=name)

    def test_unbind_ls_no_exist(self):
        name = utils.get_rand_device_name()
        switch = self._add_ls(name)
        ps = self.useFixture(fixtures.PhysicalSwitchFixture(
            self.api, name)).obj
        port = self.api.add_port(ps.uuid, name,
                                 may_exist=True).execute(check_error=True)
        self.api.bind_ls(ps.name, port.name,
                         self.vlan, switch.name).execute(check_error=True)

        for pswitch, port in [('pswitch', port.name), (ps.name, 'port')]:
            cmd = self.api.unbind_ls(pswitch, port, self.vlan)
            self.assertRaises(idlutils.RowNotFound, cmd.execute,
                              check_error=True)

    def test_unbind_ls_vlan_no_exists(self):
        name = utils.get_rand_device_name()
        switch = self._add_ls(name)
        ps = self.useFixture(fixtures.PhysicalSwitchFixture(
            self.api, name)).obj
        port = self.api.add_port(ps.uuid, name,
                                 may_exist=True).execute(check_error=True)
        self.api.bind_ls(ps.name, port.name,
                         self.vlan, switch.name).execute(check_error=True)
        vlan_bindings = port.vlan_bindings.copy()
        self.api.unbind_ls(ps.name, port.name,
                           self.vlan + 1).execute(check_error=True)
        self.assertEqual(port.vlan_bindings, vlan_bindings)


class WaitBindingsEvent(event.WaitEvent):
    def __init__(self, table, lsname, mac):
        super().__init__((self.ROW_CREATE,), table, None, None, timeout=3)
        self.lsname = lsname
        self.mac = mac

    def match_fn(self, event, row, old=None):
        # mac should really be normalized, but we pass it correctly
        return self.lsname == row.logical_switch.name and self.mac == row.MAC


class TestMacBindingsOps(HardwareVtepTest):
    def setUp(self):
        super().setUp()
        self.handler = event.RowEventHandler()
        self.api.idl.notify = self.handler.notify
        self.ls = self.useFixture(fixtures.LogicalSwitchFixture(
            self.api, utils.get_rand_device_name())).obj
        self.mac = '0a:00:d0:af:20:c0'
        self.ip = '192.168.0.1'
        self.vtep_ctl_wait("add-ucast-local", "Ucast_Macs_Local")
        self.vtep_ctl_wait("add-mcast-local", "Mcast_Macs_Local")
        self.vtep_ctl_wait("add-ucast-remote", "Ucast_Macs_Remote")
        self.vtep_ctl_wait("add-mcast-remote", "Mcast_Macs_Remote")
        for args in [
            ['vtep-ctl', 'del-ucast-local', self.ls.name, self.mac],
            ['vtep-ctl', 'del-mcast-local', self.ls.name, self.mac, self.ip],
            ['vtep-ctl', 'del-ucast-remote', self.ls.name, self.mac],
            ['vtep-ctl', 'del-mcast-remote', self.ls.name, self.mac, self.ip]
        ]:
            self.addCleanup(self.ovsvenv.call, args)

    def vtep_ctl_wait(self, cmd, table):
        wait_event = WaitBindingsEvent(table, self.ls.name, self.mac)
        self.handler.watch_event(wait_event)
        self.ovsvenv.call(["vtep-ctl", cmd, self.ls.name, self.mac, self.ip])
        self.assertTrue(wait_event.wait())

    def test_list_local_macs(self):
        local_macs = self.api.list_local_macs(
            self.ls.name).execute(check_error=True)
        for macs in local_macs:
            self.assertEqual(len(macs), 1)
            self.assertEqual(macs[0].MAC, self.mac)

    def test_list_remote_macs(self):
        remote_macs = self.api.list_remote_macs(
            self.ls.name).execute(check_error=True)
        for macs in remote_macs:
            self.assertEqual(len(macs), 1)
            self.assertEqual(macs[0].MAC, self.mac)

    def test_clear_local_macs(self):
        ucast_table = self.api.tables['Ucast_Macs_Local']
        mcast_table = self.api.tables['Mcast_Macs_Local']
        for table in [ucast_table, mcast_table]:
            self.assertEqual(len(table.rows), 1)

        self.api.clear_local_macs(self.ls.name).execute(check_error=True)
        for table in [ucast_table, mcast_table]:
            self.assertEqual(len(table.rows), 0)

    def test_clear_remote_macs(self):
        ucast_table = self.api.tables['Ucast_Macs_Remote']
        mcast_table = self.api.tables['Mcast_Macs_Remote']
        for table in [ucast_table, mcast_table]:
            self.assertEqual(len(table.rows), 1)

        self.api.clear_remote_macs(self.ls.name).execute(check_error=True)
        for table in [ucast_table, mcast_table]:
            self.assertEqual(len(table.rows), 0)
