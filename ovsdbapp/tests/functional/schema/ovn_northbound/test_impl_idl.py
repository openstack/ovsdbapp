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

import netaddr

from ovsdbapp.schema.ovn_northbound import impl_idl
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.ovn_northbound import fixtures
from ovsdbapp.tests import utils


class OvnNorthboundTest(base.FunctionalTestCase):
    schema = 'OVN_Northbound'

    def setUp(self):
        super(OvnNorthboundTest, self).setUp()
        self.api = impl_idl.OvnNbApiIdlImpl(self.connection)


class TestLogicalSwitchOps(OvnNorthboundTest):
    def setUp(self):
        super(TestLogicalSwitchOps, self).setUp()
        self.table = self.api.tables['Logical_Switch']

    def _ls_add(self, *args, **kwargs):
        fix = self.useFixture(fixtures.LogicalSwitchFixture(*args, **kwargs))
        self.assertIn(fix.obj.uuid, self.table.rows)
        return fix.obj

    def test_ls_add_no_name(self):
        self._ls_add()

    def test_ls_add_name(self):
        name = utils.get_rand_device_name()
        sw = self._ls_add(name)
        self.assertEqual(name, sw.name)

    def test_ls_add_exists(self):
        name = utils.get_rand_device_name()
        self._ls_add(name)
        cmd = self.api.ls_add(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_ls_add_may_exist(self):
        name = utils.get_rand_device_name()
        sw = self._ls_add(name)
        sw2 = self.api.ls_add(name, may_exist=True).execute(check_error=True)
        self.assertEqual(sw, sw2)

    def test_ls_add_columns(self):
        external_ids = {'mykey': 'myvalue', 'yourkey': 'yourvalue'}
        ls = self._ls_add(external_ids=external_ids)
        self.assertEqual(external_ids, ls.external_ids)

    def test_ls_del(self):
        sw = self._ls_add()
        self.api.ls_del(sw.uuid).execute(check_error=True)
        self.assertNotIn(sw.uuid, self.table.rows)

    def test_ls_del_by_name(self):
        name = utils.get_rand_device_name()
        self._ls_add(name)
        self.api.ls_del(name).execute(check_error=True)

    def test_ls_del_no_exist(self):
        name = utils.get_rand_device_name()
        cmd = self.api.ls_del(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_ls_del_if_exists(self):
        name = utils.get_rand_device_name()
        self.api.ls_del(name, if_exists=True).execute(check_error=True)

    def test_ls_list(self):
        with self.api.transaction(check_error=True):
            switches = {self._ls_add() for _ in range(3)}
        switch_set = set(self.api.ls_list().execute(check_error=True))
        self.assertTrue(switches.issubset(switch_set))


class TestAclOps(OvnNorthboundTest):
    def setUp(self):
        super(TestAclOps, self).setUp()
        self.switch = self.useFixture(fixtures.LogicalSwitchFixture()).obj

    def _acl_add(self, *args, **kwargs):
        cmd = self.api.acl_add(self.switch.uuid, *args, **kwargs)
        aclrow = cmd.execute(check_error=True)
        self.assertIn(aclrow._row, self.switch.acls)
        self.assertEqual(cmd.direction, aclrow.direction)
        self.assertEqual(cmd.priority, aclrow.priority)
        self.assertEqual(cmd.match, aclrow.match)
        self.assertEqual(cmd.action, aclrow.action)
        return aclrow

    def test_acl_add(self):
        self._acl_add('from-lport', 0, 'output == "fake_port" && ip',
                      'drop')

    def test_acl_add_exists(self):
        args = ('from-lport', 0, 'output == "fake_port" && ip', 'drop')
        self._acl_add(*args)
        self.assertRaises(RuntimeError, self._acl_add, *args)

    def test_acl_add_may_exist(self):
        args = ('from-lport', 0, 'output == "fake_port" && ip', 'drop')
        row = self._acl_add(*args)
        row2 = self._acl_add(*args, may_exist=True)
        self.assertEqual(row, row2)

    def test_acl_add_extids(self):
        external_ids = {'mykey': 'myvalue', 'yourkey': 'yourvalue'}
        acl = self._acl_add('from-lport', 0, 'output == "fake_port" && ip',
                            'drop', **external_ids)
        self.assertEqual(external_ids, acl.external_ids)

    def test_acl_del_all(self):
        r1 = self._acl_add('from-lport', 0, 'output == "fake_port"', 'drop')
        self.api.acl_del(self.switch.uuid).execute(check_error=True)
        self.assertNotIn(r1.uuid, self.api.tables['ACL'].rows)
        self.assertEqual([], self.switch.acls)

    def test_acl_del_direction(self):
        r1 = self._acl_add('from-lport', 0, 'output == "fake_port"', 'drop')
        r2 = self._acl_add('to-lport', 0, 'output == "fake_port"', 'allow')
        self.api.acl_del(self.switch.uuid, 'from-lport').execute(
            check_error=True)
        self.assertNotIn(r1, self.switch.acls)
        self.assertIn(r2, self.switch.acls)

    def test_acl_del_direction_priority_match(self):
        r1 = self._acl_add('from-lport', 0, 'output == "fake_port"', 'drop')
        r2 = self._acl_add('from-lport', 1, 'output == "fake_port"', 'allow')
        cmd = self.api.acl_del(self.switch.uuid,
                               'from-lport', 0, 'output == "fake_port"')
        cmd.execute(check_error=True)
        self.assertNotIn(r1, self.switch.acls)
        self.assertIn(r2, self.switch.acls)

    def test_acl_del_priority_without_match(self):
        self.assertRaises(TypeError, self.api.acl_del, self.switch.uuid,
                          'from-lport', 0)

    def test_acl_del_priority_without_direction(self):
        self.assertRaises(TypeError, self.api.acl_del, self.switch.uuid,
                          priority=0)

    def test_acl_list(self):
        r1 = self._acl_add('from-lport', 0, 'output == "fake_port"', 'drop')
        r2 = self._acl_add('from-lport', 1, 'output == "fake_port2"', 'allow')
        acls = self.api.acl_list(self.switch.uuid).execute(check_error=True)
        self.assertIn(r1, acls)
        self.assertIn(r2, acls)


class TestLspOps(OvnNorthboundTest):
    def setUp(self):
        super(TestLspOps, self).setUp()
        name = utils.get_rand_device_name()
        self.switch = self.useFixture(
            fixtures.LogicalSwitchFixture(name)).obj

    def _lsp_add(self, switch, name, *args, **kwargs):
        name = utils.get_rand_device_name() if name is None else name
        lsp = self.api.lsp_add(switch.uuid, name, *args, **kwargs).execute(
            check_error=True)
        self.assertIn(lsp, switch.ports)
        return lsp

    def test_lsp_add(self):
        self._lsp_add(self.switch, None)

    def test_lsp_add_exists(self):
        lsp = self._lsp_add(self.switch, None)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp.name)

    def test_lsp_add_may_exist(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, lsp1.name, may_exist=True)
        self.assertEqual(lsp1, lsp2)

    def test_lsp_add_may_exist_wrong_switch(self):
        sw = self.useFixture(fixtures.LogicalSwitchFixture()).obj
        lsp = self._lsp_add(self.switch, None)
        self.assertRaises(RuntimeError, self._lsp_add, sw, lsp.name,
                          may_exist=True)

    def test_lsp_add_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent=lsp1.name, tag=0)
        # parent_name, being optional, is stored as a list
        self.assertIn(lsp1.name, lsp2.parent_name)

    def test_lsp_add_parent_no_tag(self):
        self.assertRaises(TypeError, self._lsp_add, self.switch,
                          None, parent="fake_parent")

    def test_lsp_add_parent_may_exist(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent=lsp1.name, tag=0)
        lsp3 = self._lsp_add(self.switch, lsp2.name, parent=lsp1.name,
                             tag=0, may_exist=True)
        self.assertEqual(lsp2, lsp3)

    def test_lsp_add_parent_may_exist_no_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp1.name, parent="fake_parent", tag=0,
                          may_exist=True)

    def test_lsp_add_parent_may_exist_different_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent=lsp1.name, tag=0)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp2.name, parent="fake_parent", tag=0,
                          may_exist=True)

    def test_lsp_add_parent_may_exist_different_tag(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent=lsp1.name, tag=0)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp2.name, parent=lsp1.name, tag=1, may_exist=True)

    def test_lsp_add_may_exist_existing_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent=lsp1.name, tag=0)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp2.name, may_exist=True)

    def test_lsp_add_columns(self):
        options = {'myside': 'yourside'}
        external_ids = {'myside': 'yourside'}
        lsp = self._lsp_add(self.switch, None, options=options,
                            external_ids=external_ids)
        self.assertEqual(options, lsp.options)
        self.assertEqual(external_ids, lsp.external_ids)

    def test_lsp_del_uuid(self):
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_del(lsp.uuid).execute(check_error=True)
        self.assertNotIn(lsp, self.switch.ports)

    def test_lsp_del_name(self):
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_del(lsp.name).execute(check_error=True)
        self.assertNotIn(lsp, self.switch.ports)

    def test_lsp_del_switch(self):
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_del(lsp.uuid, self.switch.uuid).execute(check_error=True)
        self.assertNotIn(lsp, self.switch.ports)

    def test_lsp_del_switch_name(self):
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_del(lsp.uuid,
                         self.switch.name).execute(check_error=True)
        self.assertNotIn(lsp, self.switch.ports)

    def test_lsp_del_wrong_switch(self):
        lsp = self._lsp_add(self.switch, None)
        sw_id = self.useFixture(fixtures.LogicalSwitchFixture()).obj
        cmd = self.api.lsp_del(lsp.uuid, sw_id)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lsp_del_switch_no_exist(self):
        lsp = self._lsp_add(self.switch, None)
        cmd = self.api.lsp_del(lsp.uuid, utils.get_rand_device_name())
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lsp_del_no_exist(self):
        cmd = self.api.lsp_del("fake_port")
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lsp_del_if_exist(self):
        self.api.lsp_del("fake_port", if_exists=True).execute(check_error=True)

    def test_lsp_list(self):
        ports = {self._lsp_add(self.switch, None) for _ in range(3)}
        port_set = set(self.api.lsp_list(self.switch.uuid).execute(
            check_error=True))
        self.assertTrue(ports.issubset(port_set))

    def test_lsp_list_no_switch(self):
        ports = {self._lsp_add(self.switch, None) for _ in range(3)}
        other_switch = self.useFixture(fixtures.LogicalSwitchFixture(
            name=utils.get_rand_device_name())).obj
        other_port = self._lsp_add(other_switch, None)
        all_ports = set(self.api.lsp_list().execute(check_error=True))
        self.assertTrue((ports.union(set([other_port]))).issubset(all_ports))

    def test_lsp_get_parent(self):
        ls1 = self._lsp_add(self.switch, None)
        ls2 = self._lsp_add(self.switch, None, parent=ls1.name, tag=0)
        self.assertEqual(
            ls1.name, self.api.lsp_get_parent(ls2.name).execute(
                check_error=True))

    def test_lsp_get_tag(self):
        ls1 = self._lsp_add(self.switch, None)
        ls2 = self._lsp_add(self.switch, None, parent=ls1.name, tag=0)
        self.assertIsInstance(self.api.lsp_get_tag(ls2.uuid).execute(
            check_error=True), int)

    def test_lsp_set_addresses(self):
        lsp = self._lsp_add(self.switch, None)
        for addr in ('dynamic', 'unknown', 'router',
                     'de:ad:be:ef:4d:ad 192.0.2.1'):
            self.api.lsp_set_addresses(lsp.name, [addr]).execute(
                check_error=True)
            self.assertEqual([addr], lsp.addresses)

    def test_lsp_set_addresses_invalid(self):
        self.assertRaises(
            TypeError,
            self.api.lsp_set_addresses, 'fake', '01:02:03:04:05:06')

    def test_lsp_get_addresses(self):
        addresses = [
            '01:02:03:04:05:06 192.0.2.1',
            'de:ad:be:ef:4d:ad 192.0.2.2']
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_set_addresses(
            lsp.name, addresses).execute(check_error=True)
        self.assertEqual(set(addresses), set(self.api.lsp_get_addresses(
            lsp.name).execute(check_error=True)))

    def test_lsp_get_set_port_security(self):
        port_security = [
            '01:02:03:04:05:06 192.0.2.1',
            'de:ad:be:ef:4d:ad 192.0.2.2']
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_set_port_security(lsp.name, port_security).execute(
            check_error=True)
        ps = self.api.lsp_get_port_security(lsp.name).execute(
            check_error=True)
        self.assertEqual(port_security, ps)

    def test_lsp_get_up(self):
        lsp = self._lsp_add(self.switch, None)
        self.assertFalse(self.api.lsp_get_up(lsp.name).execute(
            check_error=True))

    def test_lsp_get_set_enabled(self):
        lsp = self._lsp_add(self.switch, None)
        # default is True
        self.assertTrue(self.api.lsp_get_enabled(lsp.name).execute(
            check_error=True))
        self.api.lsp_set_enabled(lsp.name, False).execute(check_error=True)
        self.assertFalse(self.api.lsp_get_enabled(lsp.name).execute(
            check_error=True))
        self.api.lsp_set_enabled(lsp.name, True).execute(check_error=True)
        self.assertTrue(self.api.lsp_get_enabled(lsp.name).execute(
            check_error=True))

    def test_lsp_get_set_type(self):
        type_ = 'router'
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_set_type(lsp.uuid, type_).execute(check_error=True)
        self.assertEqual(type_, self.api.lsp_get_type(lsp.uuid).execute(
            check_error=True))

    def test_lsp_get_set_options(self):
        options = {'one': 'two', 'three': 'four'}
        lsp = self._lsp_add(self.switch, None)
        self.api.lsp_set_options(lsp.uuid, **options).execute(
            check_error=True)
        self.assertEqual(options, self.api.lsp_get_options(lsp.uuid).execute(
            check_error=True))

    def test_lsp_set_get_dhcpv4_options(self):
        lsp = self._lsp_add(self.switch, None)
        dhcpopt = self.useFixture(
            fixtures.DhcpOptionsFixture('192.0.2.1/24')).obj
        self.api.lsp_set_dhcpv4_options(
            lsp.name, dhcpopt.uuid).execute(check_error=True)
        options = self.api.lsp_get_dhcpv4_options(
            lsp.uuid).execute(check_error=True)
        self.assertEqual(dhcpopt, options)


class TestDhcpOptionsOps(OvnNorthboundTest):
    def _dhcpopt_add(self, cidr, *args, **kwargs):
        dhcpopt = self.useFixture(fixtures.DhcpOptionsFixture(
            cidr, *args, **kwargs)).obj
        self.assertEqual(cidr, dhcpopt.cidr)
        return dhcpopt

    def test_dhcp_options_add(self):
        self._dhcpopt_add('192.0.2.1/24')

    def test_dhcp_options_add_v6(self):
        self._dhcpopt_add('2001:db8::1/32')

    def test_dhcp_options_invalid_cidr(self):
        self.assertRaises(netaddr.AddrFormatError, self.api.dhcp_options_add,
                          '256.0.0.1/24')

    def test_dhcp_options_add_ext_ids(self):
        ext_ids = {'subnet-id': '1', 'other-id': '2'}
        dhcpopt = self._dhcpopt_add('192.0.2.1/24', **ext_ids)
        self.assertEqual(ext_ids, dhcpopt.external_ids)

    def test_dhcp_options_list(self):
        dhcpopts = {self._dhcpopt_add('192.0.2.1/24') for d in range(3)}
        dhcpopts_set = set(
            self.api.dhcp_options_list().execute(check_error=True))
        self.assertTrue(dhcpopts.issubset(dhcpopts_set))

    def test_dhcp_options_get_set_options(self):
        dhcpopt = self._dhcpopt_add('192.0.2.1/24')
        options = {'a': 'one', 'b': 'two'}
        self.api.dhcp_options_set_options(
            dhcpopt.uuid, **options).execute(check_error=True)
        cmd = self.api.dhcp_options_get_options(dhcpopt.uuid)
        self.assertEqual(options, cmd.execute(check_error=True))
