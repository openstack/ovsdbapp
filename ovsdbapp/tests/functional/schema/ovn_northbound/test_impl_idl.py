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
import testscenarios
import types
import uuid

from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import constants as const
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.ovn_northbound import fixtures
from ovsdbapp.tests import utils
from ovsdbapp import utils as ovsdb_utils


class OvnNorthboundTest(base.FunctionalTestCase):
    schemas = ['OVN_Northbound']

    def setUp(self):
        super(OvnNorthboundTest, self).setUp()
        self.api = self.useFixture(fixtures.NbApiFixture(self.connection)).obj


class TestLogicalSwitchOps(OvnNorthboundTest):
    def setUp(self):
        super(TestLogicalSwitchOps, self).setUp()
        self.table = self.api.tables['Logical_Switch']

    def _ls_add(self, *args, **kwargs):
        fix = self.useFixture(fixtures.LogicalSwitchFixture(self.api, *args,
                                                            **kwargs))
        self.assertIn(fix.obj.uuid, self.table.rows)
        return fix.obj

    def _lsp_add(self, switch, name, *args, **kwargs):
        name = utils.get_rand_device_name() if name is None else name
        self.api.lsp_add(switch.uuid, name, *args, **kwargs).execute(
            check_error=True)

    def _test_ls_get(self, col):
        ls = self._ls_add(switch=utils.get_rand_device_name())
        val = getattr(ls, col)
        found = self.api.ls_get(val).execute(check_error=True)
        self.assertEqual(ls, found)

    def test_ls_get_uuid(self):
        self._test_ls_get('uuid')

    def test_ls_get_name(self):
        self._test_ls_get('name')

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
        switches = {self._ls_add() for _ in range(3)}
        switch_set = set(self.api.ls_list().execute(check_error=True))
        self.assertTrue(switches.issubset(switch_set))

    def test_ls_get_localnet_ports(self):
        ls = self._ls_add()
        self._lsp_add(ls, None, type=const.LOCALNET)
        localnet_ports = self.api.ls_get_localnet_ports(ls.uuid).execute(
            check_error=True)
        self.assertEqual(ls.ports, localnet_ports)

    def test_ls_get_localnet_ports_no_ports(self):
        ls = self._ls_add()
        localnet_ports = self.api.ls_get_localnet_ports(ls.uuid).execute(
            check_error=True)
        self.assertEqual([], localnet_ports)

    def test_ls_get_localnet_ports_no_exist(self):
        name = utils.get_rand_device_name()
        cmd = self.api.ls_get_localnet_ports(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_ls_get_localnet_ports_if_exists(self):
        name = utils.get_rand_device_name()
        localnet_ports = self.api.ls_get_localnet_ports(
            name, if_exists=True).execute(check_error=True)
        self.assertEqual([], localnet_ports)


class TestAclOps(OvnNorthboundTest):
    def setUp(self):
        super(TestAclOps, self).setUp()
        self.switch = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api)).obj
        self.port_group = self.useFixture(
            fixtures.PortGroupFixture(self.api)).obj

    def _acl_add(self, entity, *args, **kwargs):
        self.assertIn(entity, ['lswitch', 'port_group'])
        if entity == 'lswitch':
            cmd = self.api.acl_add(self.switch.uuid, *args, **kwargs)
            resource = self.switch
        else:
            cmd = self.api.pg_acl_add(self.port_group.uuid, *args, **kwargs)
            resource = self.port_group

        aclrow = cmd.execute(check_error=True)
        self.assertIn(aclrow._row, resource.acls)
        self.assertEqual(cmd.direction, aclrow.direction)
        self.assertEqual(cmd.priority, aclrow.priority)
        self.assertEqual(cmd.match, aclrow.match)
        self.assertEqual(cmd.action, aclrow.action)
        return aclrow

    def test_acl_add(self):
        self._acl_add('lswitch', 'from-lport', 0,
                      'output == "fake_port" && ip', 'drop')

    def test_acl_add_exists(self):
        args = ('lswitch', 'from-lport', 0, 'output == "fake_port" && ip',
                'drop')
        self._acl_add(*args)
        self.assertRaises(RuntimeError, self._acl_add, *args)

    def test_acl_add_may_exist(self):
        args = ('from-lport', 0, 'output == "fake_port" && ip', 'drop')
        row = self._acl_add('lswitch', *args)
        row2 = self._acl_add('lswitch', *args, may_exist=True)
        self.assertEqual(row, row2)

    def test_acl_add_extids(self):
        external_ids = {'mykey': 'myvalue', 'yourkey': 'yourvalue'}
        acl = self._acl_add('lswitch',
                            'from-lport', 0, 'output == "fake_port" && ip',
                            'drop', **external_ids)
        self.assertEqual(external_ids, acl.external_ids)

    def test_acl_del_all(self):
        r1 = self._acl_add('lswitch', 'from-lport', 0, 'output == "fake_port"',
                           'drop')
        self.api.acl_del(self.switch.uuid).execute(check_error=True)
        self.assertNotIn(r1.uuid, self.api.tables['ACL'].rows)
        self.assertEqual([], self.switch.acls)

    def test_acl_del_direction(self):
        r1 = self._acl_add('lswitch', 'from-lport', 0,
                           'output == "fake_port"', 'drop')
        r2 = self._acl_add('lswitch', 'to-lport', 0,
                           'output == "fake_port"', 'allow')
        self.api.acl_del(self.switch.uuid, 'from-lport').execute(
            check_error=True)
        self.assertNotIn(r1, self.switch.acls)
        self.assertIn(r2, self.switch.acls)

    def test_acl_del_direction_priority_match(self):
        r1 = self._acl_add('lswitch', 'from-lport', 0,
                           'output == "fake_port"', 'drop')
        r2 = self._acl_add('lswitch', 'from-lport', 1,
                           'output == "fake_port"', 'allow')
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

    def test_acl_del_acl_not_present(self):
        r1 = self._acl_add('lswitch', 'from-lport', 0,
                           'output == "fake_port"', 'drop')
        cmd = self.api.acl_del(self.switch.uuid,
                               'from-lport', 0, 'output == "fake_port"')
        cmd.execute(check_error=True)
        self.assertNotIn(r1, self.switch.acls)

        # The acl_del command is idempotent.
        cmd.execute(check_error=True)
        self.assertNotIn(r1, self.switch.acls)

    def test_acl_del_if_exists_false(self):
        cmd = self.api.acl_del('lswitch2', 'from-lport', 0, 'match')
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_acl_del_if_exists_true(self):
        self.assertIsNone(
            self.api.acl_del('lswitch2', 'from-lport', 0, 'match',
                             if_exists=True).execute(check_error=True))

    def test_acl_list(self):
        r1 = self._acl_add('lswitch', 'from-lport', 0,
                           'output == "fake_port"', 'drop')
        r2 = self._acl_add('lswitch', 'from-lport', 1,
                           'output == "fake_port2"', 'allow')
        acls = self.api.acl_list(self.switch.uuid).execute(check_error=True)
        self.assertIn(r1, acls)
        self.assertIn(r2, acls)

    def test_pg_acl_add(self):
        self._acl_add('port_group', 'from-lport', 0,
                      'output == "fake_port" && ip', 'drop')

    def test_pg_acl_add_logging(self):
        r1 = self._acl_add('port_group', 'from-lport', 0,
                           'output == "fake_port" && ip', 'drop',
                           log=True, severity="warning", name="test1",
                           meter="meter1", extkey1="extval1")
        self.assertTrue(r1.log)
        self.assertEqual(["meter1"], r1.meter)
        self.assertEqual(["warning"], r1.severity)
        self.assertEqual(["test1"], r1.name)
        self.assertEqual({"extkey1": "extval1"}, r1.external_ids)

    def test_pg_acl_add_logging_weird_severity(self):
        r1 = self._acl_add('port_group', 'from-lport', 0,
                           'output == "fake_port" && ip', 'drop',
                           severity="weird")
        self.assertNotEqual(["weird"], r1.severity)
        self.assertEqual([], r1.severity)

    def test_pg_acl_del_all(self):
        r1 = self._acl_add('port_group', 'from-lport', 0,
                           'output == "fake_port"', 'drop')
        self.api.pg_acl_del(self.port_group.uuid).execute(check_error=True)
        self.assertNotIn(r1.uuid, self.api.tables['ACL'].rows)
        self.assertEqual([], self.port_group.acls)

    def test_pg_acl_del_acl_not_present(self):
        r1 = self._acl_add('port_group', 'from-lport', 0,
                           'output == "fake_port"', 'drop')
        cmd = self.api.pg_acl_del(self.port_group.uuid)
        cmd.execute(check_error=True)
        self.assertNotIn(r1.uuid, self.api.tables['ACL'].rows)
        self.assertEqual([], self.port_group.acls)

        # The pg_acl_del command is idempotent.
        cmd.execute(check_error=True)
        self.assertNotIn(r1.uuid, self.api.tables['ACL'].rows)
        self.assertEqual([], self.port_group.acls)

    def test_pg_acl_del_if_exists_false(self):
        cmd = self.api.pg_acl_del('port_group2')
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_pg_acl_del_if_exists_true(self):
        self.assertIsNone(
            self.api.pg_acl_del('port_group2',
                                if_exists=True).execute(check_error=True))

    def test_pg_acl_list(self):
        r1 = self._acl_add('port_group', 'from-lport', 0,
                           'output == "fake_port"', 'drop')
        r2 = self._acl_add('port_group', 'from-lport', 1,
                           'output == "fake_port2"', 'allow')
        acls = self.api.pg_acl_list(self.port_group.uuid).execute(
            check_error=True)
        self.assertIn(r1, acls)
        self.assertIn(r2, acls)


class TestAddressSetOps(OvnNorthboundTest):
    def setUp(self):
        super(TestAddressSetOps, self).setUp()
        self.table = self.api.tables['Address_Set']

    def _addr_set_add(self, name=None, *args, **kwargs):
        if name is None:
            name = utils.get_rand_name()
        fix = self.useFixture(fixtures.AddressSetFixture(self.api, name,
                                                         *args, **kwargs))
        self.assertIn(fix.obj.uuid, self.table.rows)
        return fix.obj

    def _test_addr_set_get(self, col):
        addr_set = self._addr_set_add()
        val = getattr(addr_set, col)
        found = self.api.address_set_get(val).execute(check_error=True)
        self.assertEqual(addr_set, found)

    def test_addr_set_get_uuid(self):
        self._test_addr_set_get('uuid')

    def test_addr_set_get_name(self):
        self._test_addr_set_get('name')

    def test_addr_set_add_name(self):
        name = utils.get_rand_device_name()
        addr_set = self._addr_set_add(name)
        self.assertEqual(name, addr_set.name)

    def test_addr_set_add_exists(self):
        name = utils.get_rand_device_name()
        self._addr_set_add(name)
        cmd = self.api.address_set_add(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_addr_set_add_may_exist(self):
        name = utils.get_rand_device_name()
        addr_set = self._addr_set_add(name)
        addr_set2 = self.api.address_set_add(
            name, may_exist=True).execute(check_error=True)
        self.assertEqual(addr_set, addr_set2)

    def test_addr_set_add_with_addresses(self):
        addresses = ['192.168.0.1', '192.168.0.2', '192.168.10.10/32']
        addr_set = self._addr_set_add(addresses=addresses)
        self.assertEqual(addresses, addr_set.addresses)

    def test_addr_set_del(self):
        addr_set = self._addr_set_add()
        self.api.address_set_del(addr_set.uuid).execute(check_error=True)
        self.assertNotIn(addr_set.uuid, self.table.rows)

    def test_addr_set_del_by_name(self):
        name = utils.get_rand_device_name()
        self._addr_set_add(name)
        self.api.address_set_del(name).execute(check_error=True)

    def test_addr_set_del_no_exist(self):
        name = utils.get_rand_device_name()
        cmd = self.api.address_set_del(name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_addr_set_del_if_exists(self):
        name = utils.get_rand_device_name()
        self.api.address_set_del(
            name, if_exists=True).execute(check_error=True)

    def test_addr_set_list(self):
        addr_sets = {self._addr_set_add() for _ in range(3)}
        found_sets = set(self.api.address_set_list().execute(check_error=True))
        self.assertTrue(addr_sets.issubset(found_sets))

    def test_addr_set_add_addresses(self):
        addresses = ['192.168.0.1', '192.168.0.2', '192.168.10.10/32']
        addr_set = self._addr_set_add()

        self.api.address_set_add_addresses(
            addr_set.uuid, addresses).execute(check_error=True)
        self.assertEqual(addresses, addr_set.addresses)

        self.api.address_set_add_addresses(
            addr_set.uuid, addresses).execute(check_error=True)
        self.assertEqual(addresses, addr_set.addresses)

    def test_addr_set_remove_addresses(self):
        addresses = ['192.168.0.1', '192.168.0.2', '192.168.10.10/32']
        addr_set = self._addr_set_add(addresses=addresses)

        self.api.address_set_remove_addresses(
            addr_set.uuid, addresses).execute(check_error=True)
        self.assertEqual(addr_set.addresses, [])

        self.api.address_set_remove_addresses(
            addr_set.uuid, addresses).execute(check_error=True)
        self.assertEqual(addr_set.addresses, [])

    def test_addr_set_add_remove_addresses_by_str(self):
        address = "192.168.0.1"
        addr_set = self._addr_set_add()

        self.api.address_set_add_addresses(
            addr_set.uuid, address).execute(check_error=True)
        self.assertEqual([address], addr_set.addresses)

        self.api.address_set_remove_addresses(
            addr_set.uuid, address).execute(check_error=True)
        self.assertEqual([], addr_set.addresses)


class TestQoSOps(OvnNorthboundTest):
    def setUp(self):
        super(TestQoSOps, self).setUp()
        self.switch = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api, name='LS_for_QoS')).obj

    def _qos_add(self, *args, **kwargs):
        cmd = self.api.qos_add(self.switch.uuid, *args, **kwargs)
        row = cmd.execute(check_error=True)
        self.assertIn(row._row, self.switch.qos_rules)
        self.assertEqual(cmd.direction, row.direction)
        self.assertEqual(cmd.priority, row.priority)
        self.assertEqual(cmd.match, row.match)
        self.assertEqual(cmd.rate, row.bandwidth.get('rate', None))
        self.assertEqual(cmd.burst, row.bandwidth.get('burst', None))
        self.assertEqual(cmd.dscp, row.action.get('dscp', None))
        return idlutils.frozen_row(row)

    def test_qos_add_dscp(self):
        self._qos_add('from-lport', 0, 'output == "fake_port" && ip', dscp=33)

    def test_qos_add_rate(self):
        self._qos_add('from-lport', 0, 'output == "fake_port" && ip', rate=100)

    def test_qos_add_rate_burst(self):
        self._qos_add('from-lport', 0, 'output == "fake_port" && ip', rate=101,
                      burst=1001)

    def test_qos_add_rate_dscp(self):
        self._qos_add('from-lport', 0, 'output == "fake_port" && ip', rate=102,
                      burst=1002, dscp=56)

    def test_qos_add_raises(self):
        self.assertRaises(TypeError, self.api.qos_add, 'from-lport', 0,
                          'output == "fake_port" && ip')

    def test_qos_add_direction_raises(self):
        self.assertRaises(TypeError, self.api.qos_add, 'foo', 0, 'ip',
                          bandwidth={'rate': 102, 'burst': 1002})

    def test_qos_add_priority_raises(self):
        self.assertRaises(TypeError, self.api.qos_add, 'from-lport', 32768,
                          'ip', bandwidth={'rate': 102, 'burst': 1002})

    def test_qos_add_exists(self):
        args = ('from-lport', 0, 'output == "fake_port" && ip', 1000)
        self._qos_add(*args)
        self.assertRaises(RuntimeError, self._qos_add, *args)

    def test_qos_add_may_exist(self):
        args = ('from-lport', 0, 'output == "fake_port" && ip', 1000)
        row = self._qos_add(*args, external_ids={'port_id': '1'})
        row2 = self._qos_add(*args, external_ids={'port_id': '1'},
                             may_exist=True)
        self.assertEqual(row, row2)

    def test_qos_add_may_exist_update_using_external_ids_match(self):
        args = ('from-lport', 0, 'output == "fake_port" && ip')
        kwargs = {'rate': 1000, 'burst': 800, 'dscp': 16,
                  'external_ids': {'port_id': '1'}}
        row = self._qos_add(*args, **kwargs)

        # Update QoS parameters: rate, burst and DSCP.
        kwargs = {'rate': 1200, 'burst': 900, 'dscp': 24}
        row2 = self._qos_add(*args, external_ids_match={'port_id': '1'},
                             may_exist=True, **kwargs)
        self.assertEqual(row.uuid, row2.uuid)
        self.assertEqual(row2.bandwidth, {'rate': 1200, 'burst': 900})
        self.assertEqual(row2.action, {'dscp': 24})

        # Remove QoS parameters.
        kwargs = {'rate': 1500, 'burst': 1100}
        row3 = self._qos_add(*args, external_ids_match={'port_id': '1'},
                             may_exist=True, **kwargs)
        self.assertEqual(row.uuid, row3.uuid)
        self.assertEqual(row3.bandwidth, {'rate': 1500, 'burst': 1100})
        self.assertEqual(row3.action, {})

        kwargs = {'rate': 2000}
        row4 = self._qos_add(*args, external_ids_match={'port_id': '1'},
                             may_exist=True, **kwargs)
        self.assertEqual(row.uuid, row4.uuid)
        self.assertEqual(row4.bandwidth, {'rate': 2000})
        self.assertEqual(row4.action, {})

        kwargs = {'dscp': 16}
        row5 = self._qos_add(*args, external_ids_match={'port_id': '1'},
                             may_exist=True, **kwargs)
        self.assertEqual(row.uuid, row5.uuid)
        self.assertEqual(row5.bandwidth, {})
        self.assertEqual(row5.action, {'dscp': 16})

    def test_qos_add_may_exist_using_external_ids_match(self):
        _uuid = str(uuid.uuid4())
        _uuid2 = str(uuid.uuid4())
        for key in ('neutron:port_id', 'neutron:fip_id'):
            args = ('from-lport', 0, 'output == "fake_port" && ip')
            kwargs = {'rate': 1000, 'burst': 800, 'dscp': 16}
            self._qos_add(*args, external_ids={key: _uuid}, **kwargs)

            # "args" in this second call are different, "QoSAddCommand" will
            # use the "external_ids_match" reference passed instead to match
            # the QoS rules.
            args = ('from-lport', 1, 'output == "fake_port" && ip')
            self._qos_add(*args, external_ids_match={key: _uuid},
                          may_exist=True, **kwargs)
            qos_rules = self.api.qos_list(self.switch.uuid).execute(
                check_error=True)
            self.assertEqual(1, len(qos_rules))

            # This call will update the "external_ids" to "_uuid2". Before
            # changing it, "_uuid" will be used to find the register.
            self._qos_add(*args, external_ids_match={key: _uuid},
                          external_ids={key: _uuid2}, may_exist=True, **kwargs)
            qos_rules = self.api.qos_list(self.switch.uuid).execute(
                check_error=True)
            self.assertEqual(1, len(qos_rules))

            # The deletion call uses "_uuid2" because it was changed in the
            # previous call.
            self.api.qos_del_ext_ids(self.switch.name,
                                     {key: _uuid2}).execute(check_error=True)
            qos_rules = self.api.qos_list(self.switch.uuid).execute(
                check_error=True)
            self.assertEqual(0, len(qos_rules))

    def test_qos_add_extids(self):
        external_ids = {'mykey': 'myvalue', 'yourkey': 'yourvalue'}
        qos = self._qos_add('from-lport', 0, 'output == "fake_port" && ip',
                            dscp=11, external_ids=external_ids)
        self.assertEqual(external_ids, qos.external_ids)

    def test_qos_del_all(self):
        r1 = self._qos_add('from-lport', 0, 'output == "fake_port"', 1000)
        self.api.qos_del(self.switch.uuid).execute(check_error=True)
        self.assertNotIn(r1.uuid, self.api.tables['QoS'].rows)
        self.assertEqual([], self.switch.qos_rules)

    def test_qos_del_direction(self):
        r1 = self._qos_add('from-lport', 0, 'output == "fake_port"', 1000)
        r2 = self._qos_add('to-lport', 0, 'output == "fake_port"', 1000)
        self.api.qos_del(self.switch.uuid, 'from-lport').execute(
            check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in self.switch.qos_rules]
        self.assertNotIn(r1, qos_rules)
        self.assertIn(r2, qos_rules)

    def test_qos_del_direction_priority_match(self):
        r1 = self._qos_add('from-lport', 0, 'output == "fake_port"', 1000)
        r2 = self._qos_add('from-lport', 1, 'output == "fake_port"', 1000)
        cmd = self.api.qos_del(self.switch.uuid,
                               'from-lport', 0, 'output == "fake_port"')
        cmd.execute(check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in self.switch.qos_rules]
        self.assertNotIn(r1, qos_rules)
        self.assertIn(r2, qos_rules)

    def test_qos_del_priority_without_match(self):
        self.assertRaises(TypeError, self.api.qos_del, self.switch.uuid,
                          'from-lport', 0)

    def test_qos_del_priority_without_direction(self):
        self.assertRaises(TypeError, self.api.qos_del, self.switch.uuid,
                          priority=0)

    def test_qos_del_ls_not_present_if_exists_true(self):
        self.api.qos_del('some_other_ls').execute(check_error=True)

    def test_qos_del_ls_not_present_if_exists_false(self):
        cmd = self.api.qos_del('some_other_ls', if_exists=False)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_qos_list(self):
        r1 = self._qos_add('from-lport', 0, 'output == "fake_port"', 1000)
        r2 = self._qos_add('from-lport', 1, 'output == "fake_port2"', 1000)
        qos_rules = self.api.qos_list(self.switch.uuid).execute(
            check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in qos_rules]
        self.assertIn(r1, qos_rules)
        self.assertIn(r2, qos_rules)

    def _create_fip_qoses(self):
        ext_ids_1 = {'key1': 'value1', 'key2': 'value2'}
        self.qos_1 = self._qos_add('from-lport', 0, 'output == "fake_port1"',
                                   dscp=11, external_ids=ext_ids_1)
        ext_ids_2 = {'key3': 'value3', 'key4': 'value4'}
        self.qos_2 = self._qos_add('from-lport', 1, 'output == "fake_port2"',
                                   dscp=11, external_ids=ext_ids_2)
        self.qos_3 = self._qos_add('from-lport', 2, 'output == "fake_port3"',
                                   dscp=10)

    def test_qos_delete_external_ids(self):
        # NOTE(ralonsoh): the deletion method uses the LS name (first) and the
        # UUID (second), in order to check that api.lookup() is working with
        # both.
        self._create_fip_qoses()
        self.api.qos_del_ext_ids(self.switch.name,
                                 {'key1': 'value1'}).execute(check_error=True)
        qos_rules = self.api.qos_list(
            self.switch.uuid).execute(check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in qos_rules]
        self.assertCountEqual([self.qos_2, self.qos_3], qos_rules)

        self.api.qos_del_ext_ids(
            self.switch.uuid,
            {'key3': 'value3', 'key4': 'value4'}).execute(check_error=True)
        qos_rules = self.api.qos_list(
            self.switch.uuid).execute(check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in qos_rules]
        self.assertCountEqual([self.qos_3], qos_rules)

    def test_qos_delete_external_ids_wrong_keys_or_values(self):
        self._create_fip_qoses()
        self.api.qos_del_ext_ids(self.switch.name,
                                 {'key_z': 'value1'}).execute(check_error=True)
        qos_rules = self.api.qos_list(
            self.switch.uuid).execute(check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in qos_rules]
        self.assertCountEqual([self.qos_1, self.qos_2, self.qos_3], qos_rules)

        self.api.qos_del_ext_ids(self.switch.uuid,
                                 {'key1': 'value_z'}).execute(check_error=True)
        qos_rules = self.api.qos_list(
            self.switch.uuid).execute(check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in qos_rules]
        self.assertCountEqual([self.qos_1, self.qos_2, self.qos_3], qos_rules)

        self.api.qos_del_ext_ids(
            self.switch.uuid,
            {'key3': 'value3', 'key4': 'value_z'}).execute(check_error=True)
        qos_rules = self.api.qos_list(
            self.switch.uuid).execute(check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in qos_rules]
        self.assertCountEqual([self.qos_1, self.qos_2, self.qos_3], qos_rules)

    def test_qos_delete_external_ids_empty_dict(self):
        self.assertRaises(TypeError, self.api.qos_del_ext_ids,
                          self.switch.name, {})

    def test_qos_delete_external_ids_if_exists(self):
        self._create_fip_qoses()
        cmd = self.api.qos_del_ext_ids('wrong_ls_name',
                                       {'key1': 'value1'}, if_exists=False)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

        self.api.qos_del_ext_ids('wrong_ls_name',
                                 {'key1': 'value1'}).execute(check_error=True)
        # No qos rule has been deleted from the correct logical switch.
        qos_rules = self.api.qos_list(
            self.switch.uuid).execute(check_error=True)
        qos_rules = [idlutils.frozen_row(row) for row in qos_rules]
        self.assertCountEqual([self.qos_1, self.qos_2, self.qos_3], qos_rules)


class TestLspOps(OvnNorthboundTest):
    def setUp(self):
        super(TestLspOps, self).setUp()
        name = utils.get_rand_device_name()
        self.switch = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api, name)).obj

    def _lsp_add(self, switch, name, *args, **kwargs):
        name = utils.get_rand_device_name() if name is None else name
        lsp = self.api.lsp_add(switch.uuid, name, *args, **kwargs).execute(
            check_error=True)
        self.assertIn(lsp, switch.ports)
        return lsp

    def _test_lsp_get(self, col):
        lsp = self._lsp_add(self.switch, None)
        val = getattr(lsp, col)
        found = self.api.lsp_get(val).execute(check_error=True)
        self.assertEqual(lsp, found)

    def test_lsp_get_uuid(self):
        self._test_lsp_get('uuid')

    def test_ls_get_name(self):
        self._test_lsp_get('name')

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
        sw = self.useFixture(fixtures.LogicalSwitchFixture(self.api)).obj
        lsp = self._lsp_add(self.switch, None)
        self.assertRaises(RuntimeError, self._lsp_add, sw, lsp.name,
                          may_exist=True)

    def test_lsp_add_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent_name=lsp1.name, tag=0)
        # parent_name, being optional, is stored as a list
        self.assertIn(lsp1.name, lsp2.parent_name)

    def test_lsp_add_parent_no_tag(self):
        self.assertRaises(TypeError, self._lsp_add, self.switch,
                          None, parent_name="fake_parent")

    def test_lsp_add_parent_may_exist(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent_name=lsp1.name, tag=0)
        lsp3 = self._lsp_add(self.switch, lsp2.name, parent_name=lsp1.name,
                             tag=0, may_exist=True)
        self.assertEqual(lsp2, lsp3)

    def test_lsp_add_parent_may_exist_no_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp1.name, parent_name="fake_parent", tag=0,
                          may_exist=True)

    def test_lsp_add_parent_may_exist_different_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent_name=lsp1.name, tag=0)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp2.name, parent_name="fake_parent", tag=0,
                          may_exist=True)

    def test_lsp_add_parent_may_exist_different_tag(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent_name=lsp1.name, tag=0)
        self.assertRaises(RuntimeError, self._lsp_add, self.switch,
                          lsp2.name, parent_name=lsp1.name, tag=1,
                          may_exist=True)

    def test_lsp_add_may_exist_existing_parent(self):
        lsp1 = self._lsp_add(self.switch, None)
        lsp2 = self._lsp_add(self.switch, None, parent_name=lsp1.name, tag=0)
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
        sw = self.useFixture(fixtures.LogicalSwitchFixture(self.api)).obj
        cmd = self.api.lsp_del(lsp.uuid, sw.uuid)
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
            self.api, name=utils.get_rand_device_name())).obj
        other_port = self._lsp_add(other_switch, None)
        all_ports = set(self.api.lsp_list().execute(check_error=True))
        self.assertTrue((ports.union(set([other_port]))).issubset(all_ports))

    def test_lsp_get_parent(self):
        ls1 = self._lsp_add(self.switch, None)
        ls2 = self._lsp_add(self.switch, None, parent_name=ls1.name, tag=0)
        self.assertEqual(
            ls1.name, self.api.lsp_get_parent(ls2.name).execute(
                check_error=True))

    def test_lsp_get_tag(self):
        ls1 = self._lsp_add(self.switch, None)
        ls2 = self._lsp_add(self.switch, None, parent_name=ls1.name, tag=0)
        self.assertIsInstance(self.api.lsp_get_tag(ls2.uuid).execute(
            check_error=True), int)

    def test_lsp_set_addresses(self):
        lsp = self._lsp_add(self.switch, None)
        for addr in ('dynamic', 'unknown', 'router', 'de:ad:be:ef:4d:ad',
                     'de:ad:be:ef:4d:ad 192.0.2.1'):
            self.api.lsp_set_addresses(lsp.name, [addr]).execute(
                check_error=True)
            self.assertEqual([addr], lsp.addresses)

    def test_lsp_set_addresses_invalid(self):
        self.assertRaises(
            TypeError,
            self.api.lsp_set_addresses, 'fake', ['invalidaddress'])

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
            fixtures.DhcpOptionsFixture(self.api, '192.0.2.1/24')).obj
        self.api.lsp_set_dhcpv4_options(
            lsp.name, dhcpopt.uuid).execute(check_error=True)
        options = self.api.lsp_get_dhcpv4_options(
            lsp.uuid).execute(check_error=True)
        self.assertEqual(dhcpopt, options)


class TestDhcpOptionsOps(OvnNorthboundTest):
    def _dhcpopt_add(self, cidr, *args, **kwargs):
        dhcpopt = self.useFixture(fixtures.DhcpOptionsFixture(
            self.api, cidr, *args, **kwargs)).obj
        self.assertEqual(cidr, dhcpopt.cidr)
        return dhcpopt

    def test_dhcp_options_get(self):
        dhcpopt = self._dhcpopt_add('192.0.2.1/24')
        found = self.api.dhcp_options_get(dhcpopt.uuid).execute(
            check_error=True)
        self.assertEqual(dhcpopt, found)

    def test_dhcp_options_get_no_exist(self):
        cmd = self.api.dhcp_options_get("noexist")
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

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


class TestLogicalRouterOps(OvnNorthboundTest):
    lr_policy_match1 = 'ip4.src==10.1.1.0/24 && ip4.dst==20.1.1.0/24'
    lr_policy_match2 = 'ip4.src==30.1.1.0/24 && ip4.dst==40.1.1.0/24'

    def _lr_add(self, *args, **kwargs):
        lr = self.useFixture(
            fixtures.LogicalRouterFixture(self.api, *args, **kwargs)).obj
        self.assertIn(lr.uuid, self.api.tables['Logical_Router'].rows)
        return lr

    def test_lr_add(self):
        self._lr_add()

    def test_lr_add_name(self):
        name = utils.get_rand_device_name()
        lr = self._lr_add(name)
        self.assertEqual(name, lr.name)

    def test_lr_add_columns(self):
        external_ids = {'mykey': 'myvalue', 'yourkey': 'yourvalue'}
        lr = self._lr_add(external_ids=external_ids)
        self.assertEqual(external_ids, lr.external_ids)

    def test_lr_del(self):
        lr = self._lr_add()
        self.api.lr_del(lr.uuid).execute(check_error=True)
        self.assertNotIn(lr.uuid,
                         self.api.tables['Logical_Router'].rows.keys())

    def test_lr_del_name(self):
        lr = self._lr_add(utils.get_rand_device_name())
        self.api.lr_del(lr.name).execute(check_error=True)
        self.assertNotIn(lr.uuid,
                         self.api.tables['Logical_Router'].rows.keys())

    def test_lr_list(self):
        lrs = {self._lr_add() for _ in range(3)}
        lr_set = set(self.api.lr_list().execute(check_error=True))
        self.assertTrue(lrs.issubset(lr_set), "%s vs %s" % (lrs, lr_set))

    def _lr_add_route(self, router=None, prefix=None, nexthop=None, port=None,
                      ecmp=False, route_table=const.MAIN_ROUTE_TABLE,
                      **kwargs):
        lr = self._lr_add(router or utils.get_rand_device_name(),
                          may_exist=True)
        prefix = prefix or '192.0.2.0/25'
        nexthop = nexthop or '192.0.2.254'
        port = port or "port_name"
        sr = self.api.lr_route_add(
            lr.uuid, prefix, nexthop, port,
            ecmp=ecmp, route_table=route_table,
            **kwargs
        ).execute(check_error=True)
        self.assertIn(sr, lr.static_routes)
        self.assertEqual(prefix, sr.ip_prefix)
        self.assertEqual(nexthop, sr.nexthop)
        self.assertIn(port, sr.output_port)
        sr.router = lr
        return sr

    def test_lr_route_add(self):
        self._lr_add_route()

    def test_lr_route_add_invalid_prefix(self):
        self.assertRaises(netaddr.AddrFormatError, self._lr_add_route,
                          prefix='192.168.1.1/40')

    def test_lr_route_add_invalid_nexthop(self):
        self.assertRaises(netaddr.AddrFormatError, self._lr_add_route,
                          nexthop='256.0.1.3')

    def test_lr_route_add_exist(self):
        router_name = utils.get_rand_device_name()
        self._lr_add_route(router_name)
        self.assertRaises(RuntimeError, self._lr_add_route, router=router_name)

    def test_lr_route_add_may_exist(self):
        router_name = utils.get_rand_device_name()
        self._lr_add_route(router_name)
        self._lr_add_route(router_name, may_exist=True)

    def test_lr_route_add_exists_ecmp(self):
        router_name = utils.get_rand_device_name()
        self._lr_add_route(router_name)
        self._lr_add_route(router=router_name, nexthop='192.0.3.254',
                           ecmp=True)

    def test_lr_route_add_discard(self):
        self._lr_add_route(nexthop=const.ROUTE_DISCARD)
        self.assertRaises(netaddr.AddrFormatError, self._lr_add_route,
                          prefix='not-discard')

    def test_lr_route_add_route_table(self):
        lr = self._lr_add()
        route_table = "route-table"

        # add route to 'main' route table
        route = self._lr_add_route(lr.name)
        self.assertEqual(route.route_table, const.MAIN_ROUTE_TABLE)

        route = self._lr_add_route(lr.name, route_table=route_table)
        self.assertEqual(route.route_table, route_table)

        self.assertEqual(
            len(self.api.tables['Logical_Router_Static_Route'].rows), 2)

    def test_lr_route_add_learned_route_exist(self):
        router_name = utils.get_rand_device_name()

        learned_route = self._lr_add_route(router_name)
        self.api.db_set(
            'Logical_Router_Static_Route', learned_route.uuid,
            ('external_ids', {'ic-learned-route': str(uuid.uuid4())})).execute(
                check_error=True)

        route = self._lr_add_route(router_name)

        self.assertNotEqual(learned_route.uuid, route.uuid)
        self.assertNotIn("ic-learned-route", route.external_ids)

    def test_lr_route_add_with_bfd(self):
        router_name = utils.get_rand_device_name()
        data = utils.get_rand_name()
        bfd = self.api.bfd_add(data, data).execute(check_error=True)

        route = self._lr_add_route(router_name, bfd=bfd.uuid)

        self.assertEqual(bfd, route.bfd[0])

    def test_lr_route_del(self):
        prefix = "192.0.2.0/25"
        route = self._lr_add_route(prefix=prefix)
        self.api.lr_route_del(route.router.uuid, prefix).execute(
            check_error=True)
        self.assertNotIn(route, route.router.static_routes)

    def test_lr_route_del_all(self):
        router = self._lr_add()
        for p in range(3):
            self._lr_add_route(router.uuid, prefix="192.0.%s.0/24" % p)
        self.api.lr_route_del(router.uuid).execute(check_error=True)
        self.assertEqual([], router.static_routes)

    def test_lr_route_del_no_router(self):
        cmd = self.api.lr_route_del("fake_router", '192.0.2.0/25')
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lr_route_del_no_exist(self):
        lr = self._lr_add()
        cmd = self.api.lr_route_del(lr.uuid, '192.0.2.0/25')
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lr_route_del_if_exist(self):
        lr = self._lr_add()
        self.api.lr_route_del(lr.uuid, '192.0.2.0/25', if_exists=True).execute(
            check_error=True)

    def test_lr_route_del_ecmp(self):
        prefix = "10.0.0.0/24"
        nexthop1 = "1.1.1.1"
        nexthop2 = "2.2.2.2"
        lr = self._lr_add()

        self._lr_add_route(lr.uuid, prefix=prefix, nexthop=nexthop1)
        ecmp_route = self._lr_add_route(lr.uuid, prefix=prefix,
                                        nexthop=nexthop2, ecmp=True)
        self.assertEqual(
            len(self.api.tables['Logical_Router_Static_Route'].rows), 2)

        self.api.lr_route_del(lr.uuid, prefix, nexthop=nexthop2).execute(
            check_error=True)

        self.assertNotIn(
            ecmp_route.uuid,
            len(self.api.tables['Logical_Router_Static_Route'].rows),
        )

        cmd = self.api.lr_route_del(lr.uuid, prefix, nexthop=nexthop2)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

        self.assertEqual(
            len(self.api.tables['Logical_Router_Static_Route'].rows), 1)

    def test_lr_route_del_route_table(self):
        lr = self._lr_add()
        route_table = "route-table"

        route_in_main = self._lr_add_route(lr.uuid, prefix="10.0.0.0/24")
        route = self._lr_add_route(
            lr.uuid, prefix="10.0.1.0/24", route_table=route_table)

        self.assertEqual(len(lr.static_routes), 2)

        # try to delete from the 'main' table implicitly
        cmd = self.api.lr_route_del(lr.uuid, route.ip_prefix)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

        self.api.lr_route_del(
            lr.uuid, prefix=route.ip_prefix, route_table=route_table
        ).execute(check_error=True)
        self.assertEqual(len(lr.static_routes), 1)

        self.api.lr_route_del(
            lr.uuid, route_in_main.ip_prefix).execute(check_error=True)
        self.assertEqual(len(lr.static_routes), 0)

    def test_lr_route_list(self):
        lr = self._lr_add()
        routes = {self._lr_add_route(lr.uuid, prefix="192.0.%s.0/25" % p)
                  for p in range(3)}
        route_set = set(self.api.lr_route_list(lr.uuid).execute(
            check_error=True))
        self.assertTrue(routes.issubset(route_set))

    def test_lr_route_list_route_table(self):
        lr = self._lr_add()
        route_table = "route-table"

        prefix1 = "10.0.0.0/24"
        prefix2 = "10.0.1.0/24"

        self._lr_add_route(lr.uuid, prefix=prefix1)
        self._lr_add_route(lr.uuid, prefix=prefix2, route_table=route_table)

        routes = self.api.lr_route_list(lr.uuid).execute(check_error=True)
        self.assertEqual(len(routes), 2)  # all routes in logical router

        for route_table, prefix in zip(
            [const.MAIN_ROUTE_TABLE, route_table],
            [prefix1, prefix2]
        ):
            routes = self.api.lr_route_list(
                lr.uuid, route_table=route_table).execute(check_error=True)
            self.assertEqual(len(routes), 1)
            self.assertEqual(routes[0].ip_prefix, prefix)
            self.assertEqual(routes[0].route_table, route_table)

    def _lr_nat_add(self, *args, **kwargs):
        lr = kwargs.pop('router', self._lr_add(utils.get_rand_device_name()))
        nat = self.api.lr_nat_add(
            lr.uuid, *args, **kwargs).execute(
            check_error=True)
        self.assertIn(nat, lr.nat)
        nat.router = lr
        return nat

    def test_lr_nat_add_dnat(self):
        ext, log = ('10.172.4.1', '192.0.2.1')
        nat = self._lr_nat_add(const.NAT_DNAT, ext, log)
        self.assertEqual(ext, nat.external_ip)
        self.assertEqual(log, nat.logical_ip)

    def test_lr_nat_add_snat(self):
        ext, log = ('10.172.4.1', '192.0.2.0/24')
        nat = self._lr_nat_add(const.NAT_SNAT, ext, log)
        self.assertEqual(ext, nat.external_ip)
        self.assertEqual(log, nat.logical_ip)

    def test_lr_nat_add_port(self):
        sw = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api)).obj
        lsp = self.api.lsp_add(sw.uuid, utils.get_rand_device_name()).execute(
            check_error=True)
        lport, mac = (lsp.name, 'de:ad:be:ef:4d:ad')
        nat = self._lr_nat_add(const.NAT_BOTH, '10.172.4.1', '192.0.2.1',
                               lport, mac)
        self.assertIn(lport, nat.logical_port)  # because optional
        self.assertIn(mac, nat.external_mac)

    def test_lr_nat_add_port_no_mac(self):
        # yes, this and other TypeError tests are technically unit tests
        self.assertRaises(TypeError, self.api.lr_nat_add, 'faker',
                          const.NAT_DNAT, '10.17.4.1', '192.0.2.1', 'fake')

    def test_lr_nat_add_port_wrong_type(self):
        for nat_type in (const.NAT_DNAT, const.NAT_SNAT):
            self.assertRaises(
                TypeError, self.api.lr_nat_add, 'faker', nat_type,
                '10.17.4.1', '192.0.2.1', 'fake', 'de:ad:be:ef:4d:ad')

    def test_lr_nat_add_exists(self):
        args = (const.NAT_SNAT, '10.17.4.1', '192.0.2.0/24')
        nat1 = self._lr_nat_add(*args)
        cmd = self.api.lr_nat_add(nat1.router.uuid, *args)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lr_nat_add_may_exist(self):
        sw = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api)).obj
        lsp = self.api.lsp_add(sw.uuid, utils.get_rand_device_name()).execute(
            check_error=True)
        args = (const.NAT_BOTH, '10.17.4.1', '192.0.2.1')
        nat1 = self._lr_nat_add(*args)
        lp, mac = (lsp.name, 'de:ad:be:ef:4d:ad')
        nat2 = self.api.lr_nat_add(
            nat1.router.uuid, *args, logical_port=lp,
            external_mac=mac, may_exist=True).execute(check_error=True)
        self.assertEqual(nat1, nat2)
        self.assertIn(lp, nat2.logical_port)  # because optional
        self.assertIn(mac, nat2.external_mac)

    def test_lr_nat_add_may_exist_remove_port(self):
        sw = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api)).obj
        lsp = self.api.lsp_add(sw.uuid, utils.get_rand_device_name()).execute(
            check_error=True)
        args = (const.NAT_BOTH, '10.17.4.1', '192.0.2.1')
        lp, mac = (lsp.name, 'de:ad:be:ef:4d:ad')
        nat1 = self._lr_nat_add(*args, logical_port=lp, external_mac=mac)
        nat2 = self.api.lr_nat_add(
            nat1.router.uuid, *args, may_exist=True).execute(check_error=True)
        self.assertEqual(nat1, nat2)
        self.assertEqual([], nat2.logical_port)  # because optional
        self.assertEqual([], nat2.external_mac)

    def _three_nats(self):
        lr = self._lr_add(utils.get_rand_device_name())
        for n, nat_type in enumerate((const.NAT_DNAT, const.NAT_SNAT,
                                     const.NAT_BOTH)):
            nat_kwargs = {'router': lr, 'nat_type': nat_type,
                          'logical_ip': '10.17.4.%s' % (n + 1),
                          'external_ip': '192.0.2.%s' % (n + 1)}
            self._lr_nat_add(**nat_kwargs)
        return lr

    def _lr_nat_del(self, *args, **kwargs):
        lr = self._three_nats()
        self.api.lr_nat_del(lr.name, *args, **kwargs).execute(check_error=True)
        return lr

    def test_lr_nat_del_all(self):
        lr = self._lr_nat_del()
        self.assertEqual([], lr.nat)

    def test_lr_nat_del_type(self):
        lr = self._lr_nat_del(nat_type=const.NAT_SNAT)
        types = tuple(nat.type for nat in lr.nat)
        self.assertNotIn(const.NAT_SNAT, types)
        self.assertEqual(len(types), len(const.NAT_TYPES) - 1)

    def test_lr_nat_del_specific_dnat(self):
        lr = self._lr_nat_del(nat_type=const.NAT_DNAT, match_ip='192.0.2.1')
        self.assertEqual(len(lr.nat), len(const.NAT_TYPES) - 1)
        for nat in lr.nat:
            self.assertNotEqual('192.0.2.1', nat.external_ip)
            self.assertNotEqual(const.NAT_DNAT, nat.type)

    def test_lr_nat_del_specific_snat(self):
        lr = self._lr_nat_del(nat_type=const.NAT_SNAT, match_ip='10.17.4.2')
        self.assertEqual(len(lr.nat), len(const.NAT_TYPES) - 1)
        for nat in lr.nat:
            self.assertNotEqual('10.17.4.2', nat.external_ip)
            self.assertNotEqual(const.NAT_SNAT, nat.type)

    def test_lr_nat_del_specific_both(self):
        lr = self._lr_nat_del(nat_type=const.NAT_BOTH, match_ip='192.0.2.3')
        self.assertEqual(len(lr.nat), len(const.NAT_TYPES) - 1)
        for nat in lr.nat:
            self.assertNotEqual('192.0.2.3', nat.external_ip)
            self.assertNotEqual(const.NAT_BOTH, nat.type)

    def test_lr_nat_del_specific_not_found(self):
        self.assertRaises(idlutils.RowNotFound, self._lr_nat_del,
                          nat_type=const.NAT_BOTH, match_ip='10.17.4.2')

    def test_lr_nat_del_specific_if_exists(self):
        lr = self._lr_nat_del(nat_type=const.NAT_BOTH, match_ip='10.17.4.2',
                              if_exists=True)
        self.assertEqual(len(lr.nat), len(const.NAT_TYPES))

    def test_lr_nat_del_specific_snat_ip_network(self):
        lr = self._lr_add(utils.get_rand_device_name())
        self._lr_nat_add(router=lr,
                         nat_type=const.NAT_SNAT,
                         logical_ip='10.17.4.0/24',
                         external_ip='192.0.2.2')
        # Attempt to delete NAT rule of type const.NAT_SNAT by passing
        # an IP network (corresponding to logical_ip) as match_ip
        self.api.lr_nat_del(lr.name,
                            nat_type=const.NAT_SNAT,
                            match_ip='10.17.4.0/24').execute(check_error=True)
        # Assert that the NAT rule of type const.NAT_SNAT is deleted
        self.assertEqual([], lr.nat)

    def test_lr_nat_del_specific_snat_ip_network_not_found(self):
        self.assertRaises(idlutils.RowNotFound, self._lr_nat_del,
                          nat_type=const.NAT_SNAT, match_ip='10.17.4.0/24')

    def test_lr_nat_del_specific_dnat_ip_network(self):
        self.assertRaises(ValueError, self._lr_nat_del,
                          nat_type=const.NAT_DNAT, match_ip='192.0.2.1/32')

    def test_lr_nat_del_specific_both_ip_network(self):
        self.assertRaises(ValueError, self._lr_nat_del,
                          nat_type=const.NAT_BOTH, match_ip='192.0.2.0/24')

    def test_lr_nat_list(self):
        lr = self._three_nats()
        nats = self.api.lr_nat_list(lr.uuid).execute(check_error=True)
        self.assertEqual(lr.nat, nats)

    def _lr_policy_add(self, priority, match, action, *args, **kwargs):
        lr = kwargs.pop('router', self._lr_add(utils.get_rand_device_name()))
        policy = self.api.lr_policy_add(
            lr.uuid, priority, match, action, *args, **kwargs).execute(
            check_error=True)
        self.assertIn(policy, lr.policies)
        self.assertEqual(action, policy.action)
        self.assertEqual(match, policy.match)
        self.assertEqual(priority, policy.priority)
        policy.router = lr
        return policy

    def test_lr_policy_add_allow(self):
        priority = 10
        action = const.POLICY_ACTION_ALLOW
        policy = self._lr_policy_add(priority, self.lr_policy_match1, action)
        self.assertEqual(policy.nexthop, [])
        self.assertEqual(policy.nexthops, [])

    def test_lr_policy_add_default_chain(self):
        if not idlutils.table_has_column(self.api.idl, 'Logical_Router_Policy',
                                         'chain'):
            self.skipTest('Chain column not supported in '
                          'Logical_Router_Policy schema')
        priority = 10
        action = const.POLICY_ACTION_ALLOW
        policy = self._lr_policy_add(priority, self.lr_policy_match1, action)
        self.assertEqual(policy.chain, [])  # for default chain, chain is empty

    def test_lr_policy_add_custom_chain(self):
        if not idlutils.table_has_column(self.api.idl, 'Logical_Router_Policy',
                                         'chain'):
            self.skipTest('Chain column not supported in '
                          'Logical_Router_Policy schema')
        priority = 10
        action = const.POLICY_ACTION_ALLOW
        chain = 'custom-chain'
        policy = self._lr_policy_add(priority, self.lr_policy_match1, action,
                                     chain=chain)
        self.assertEqual(policy.chain, [chain])

    def test_lr_policy_add_same_priority_match_different_chains(self):
        if not idlutils.table_has_column(self.api.idl, 'Logical_Router_Policy',
                                         'chain'):
            self.skipTest('Chain column not supported in '
                          'Logical_Router_Policy schema')
        priority = 10
        action = const.POLICY_ACTION_ALLOW
        policy1 = self._lr_policy_add(priority, self.lr_policy_match1, action,
                                      chain='chain1')
        policy2 = self._lr_policy_add(priority, self.lr_policy_match1, action,
                                      chain='chain2')
        self.assertNotEqual(policy1.uuid, policy2.uuid)

    def test_lr_policy_add_jump(self):
        if not idlutils.table_has_column(self.api.idl, 'Logical_Router_Policy',
                                         'chain'):
            self.skipTest('Chain column not supported in '
                          'Logical_Router_Policy schema')
        priority = 10
        action = const.POLICY_ACTION_JUMP
        policy = self._lr_policy_add(priority, self.lr_policy_match1, action,
                                     jump_chain='jump-chain')
        self.assertEqual(policy.action, action)
        self.assertEqual(policy.jump_chain, ['jump-chain'])

    def test_lr_policy_add_jump_custom_chain(self):
        if not idlutils.table_has_column(self.api.idl, 'Logical_Router_Policy',
                                         'chain'):
            self.skipTest('Chain column not supported in '
                          'Logical_Router_Policy schema')
        priority = 10
        action = const.POLICY_ACTION_JUMP
        policy = self._lr_policy_add(priority,
                                     self.lr_policy_match1, action,
                                     chain='custom-chain',
                                     jump_chain='jump-chain')
        self.assertEqual(policy.action, action)
        self.assertEqual(policy.jump_chain, ['jump-chain'])
        self.assertEqual(policy.chain, ['custom-chain'])

    def test_lr_policy_add_drop(self):
        priority = 10
        action = const.POLICY_ACTION_DROP
        policy = self._lr_policy_add(priority, self.lr_policy_match1, action)
        self.assertEqual(policy.nexthop, [])
        self.assertEqual(policy.nexthops, [])

    def test_lr_policy_add_reroute_nexthop(self):
        priority = 10
        action = const.POLICY_ACTION_REROUTE
        nexthop = "10.3.0.2"
        policy = self._lr_policy_add(priority, self.lr_policy_match1, action,
                                     nexthop=nexthop)
        self.assertEqual(policy.nexthop, [nexthop])

    def test_lr_policy_add_reroute_nexthops(self):
        priority = 10
        action = const.POLICY_ACTION_REROUTE
        nexthops = ["10.3.0.2", "10.3.0.3"]
        policy = self._lr_policy_add(priority, self.lr_policy_match1, action,
                                     nexthops=nexthops)
        self.assertEqual(policy.nexthops, nexthops)

    def test_lr_policy_add_invalid_action(self):
        priority = 10
        action = "allow-related"
        self.assertRaises(TypeError, self._lr_policy_add,
                          priority, self.lr_policy_match1, action)

    def test_lr_policy_add_invalid_priority(self):
        priority = -1
        action = "allow"
        self.assertRaises(ValueError, self._lr_policy_add,
                          priority, self.lr_policy_match1, action)

    def test_lr_policy_add_reroute_no_nexthop(self):
        priority = 10
        action = const.POLICY_ACTION_REROUTE
        self.assertRaises(ValueError, self._lr_policy_add,
                          priority, self.lr_policy_match1, action)

    def test_lr_policy_add_nexthop_not_reroute(self):
        priority = 10
        action = const.POLICY_ACTION_ALLOW
        nexthop = "10.3.0.2"
        self.assertRaises(ValueError, self._lr_policy_add, priority,
                          self.lr_policy_match1, action, nexthop=nexthop)

    def test_lr_policy_add_jump_no_jump_chain(self):
        priority = 10
        action = const.POLICY_ACTION_JUMP
        self.assertRaises(ValueError, self._lr_policy_add,
                          priority, self.lr_policy_match1, action)

    def test_lr_policy_add_nexthops_not_reroute(self):
        priority = 10
        action = const.POLICY_ACTION_ALLOW
        nexthops = ["10.3.0.2", "10.3.0.3"]
        self.assertRaises(ValueError, self._lr_policy_add, priority,
                          self.lr_policy_match1, action, nexthops=nexthops)

    def test_lr_policy_may_exist(self):
        priority = 10
        action = const.POLICY_ACTION_ALLOW
        policy_init = self._lr_policy_add(priority, self.lr_policy_match1,
                                          action)
        self.assertEqual(policy_init.nexthop, [])

        # Update the policy
        action = const.POLICY_ACTION_REROUTE
        nexthop = "10.3.0.2"
        policy_new = self._lr_policy_add(priority, self.lr_policy_match1,
                                         action, nexthop=nexthop,
                                         may_exist=True,
                                         router=policy_init.router)
        self.assertEqual(policy_init, policy_new)
        self.assertEqual(policy_new.nexthop, [nexthop])

    def _three_policies(self):
        lr = self._lr_add(utils.get_rand_device_name())
        pr1 = 10
        pr2 = 9
        self._lr_policy_add(pr1, self.lr_policy_match1,
                            const.POLICY_ACTION_ALLOW, router=lr)
        self._lr_policy_add(pr2, self.lr_policy_match2,
                            const.POLICY_ACTION_DROP, router=lr)
        self._lr_policy_add(pr1, self.lr_policy_match2,
                            const.POLICY_ACTION_REROUTE,
                            nexthops=['31.1.1.2', '31.1.1.3'], router=lr)
        return lr

    def _lr_policy_del(self, *args, **kwargs):
        lr = self._three_policies()
        self.api.lr_policy_del(lr.name,
                               *args,
                               **kwargs).execute(check_error=True)
        return lr

    def test_lr_policy_del_all(self):
        lr = self._lr_policy_del()
        self.assertEqual(lr.policies, [])

    def test_lr_policy_del_priority(self):
        priority = 10
        lr = self._lr_policy_del(priority=priority)
        self.assertEqual(len(lr.policies), 1)
        self.assertEqual(lr.policies[0].action, const.POLICY_ACTION_DROP)

    def test_lr_policy_del_match_without_priority(self):
        self.assertRaises(TypeError, self._lr_policy_del,
                          match='ip4.dst==40.1.1.0/24')

    def test_lr_policy_del_specific(self):
        priority = 10
        lr = self._lr_policy_del(match=self.lr_policy_match2,
                                 priority=priority)
        for policy in lr.policies:
            self.assertNotEqual(policy.action, const.POLICY_ACTION_REROUTE)
            self.assertEqual(policy.nexthops, [])

    def test_lr_policy_del_not_found(self):
        self.assertRaises(RuntimeError, self._lr_policy_del,
                          priority=100, match='ip4.dst==40.1.1.0/24')

    def test_lr_policy_del_if_exists(self):
        lr = self._lr_policy_del(priority=100, match='ip4.dst==40.1.1.0/24',
                                 if_exists=True)
        self.assertEqual(len(lr.policies), 3)

    def test_lr_policy_del_by_chain(self):
        if not idlutils.table_has_column(self.api.idl, 'Logical_Router_Policy',
                                         'chain'):
            self.skipTest('Chain column not supported in '
                          'Logical_Router_Policy schema')
        lr = self._three_policies()

        initial_count = len(lr.policies)
        self.assertEqual(initial_count, 3)

        custom_chains = ['custom-chain-1', 'custom-chain-2', 'custom-chain-3']
        custom_policies = []

        for i, chain in enumerate(custom_chains):
            policy = self._lr_policy_add(
                20 + i, self.lr_policy_match1, const.POLICY_ACTION_ALLOW,
                chain=chain, router=lr,
            )
            custom_policies.append(policy)

        # 6 policies (3 original + 3 custom)
        self.assertEqual(len(lr.policies), initial_count + len(custom_chains))

        for i, chain in enumerate(custom_chains):
            current_count = len(lr.policies)
            expected_count = initial_count + len(custom_chains) - i
            self.assertEqual(current_count, expected_count)

            # Delete the policy with the specific chain
            self.api.lr_policy_del(lr.uuid, chain=chain).execute(
                check_error=True)

            self.assertEqual(len(lr.policies), current_count - 1)
            self.assertNotIn(custom_policies[i], lr.policies)

        # Only 3 original policies left
        self.assertEqual(len(lr.policies), initial_count)
        for policy in custom_policies:
            self.assertNotIn(policy, lr.policies)

    def test_lr_policy_list(self):
        lr = self._three_policies()
        policies = self.api.lr_policy_list(lr.uuid).execute(check_error=True)
        self.assertEqual(lr.policies, policies)

    def test_lr_policy_list_by_chain(self):
        if not idlutils.table_has_column(self.api.idl, 'Logical_Router_Policy',
                                         'chain'):
            self.skipTest('Chain column not supported in '
                          'Logical_Router_Policy schema')
        lr = self._three_policies()
        self._lr_policy_add(20,
                            self.lr_policy_match1,
                            const.POLICY_ACTION_ALLOW,
                            chain='custom-chain',
                            router=lr)

        # 4 policies in total: 3 default + 1 custom
        all_policies = self.api.lr_policy_list(lr.uuid).execute(
            check_error=True)
        self.assertEqual(len(all_policies), 4)

        # 3 default policies in 'default' chain
        default_policies = self.api.lr_policy_list(
            lr.uuid, chain=const.DEFAULT_CHAIN).execute(check_error=True)
        self.assertEqual(len(default_policies), 3)

        # 1 custom policy in 'custom-chain'
        custom_policies = self.api.lr_policy_list(
            lr.uuid, chain='custom-chain').execute(check_error=True)
        self.assertEqual(len(custom_policies), 1)


class TestLogicalRouterPortOps(OvnNorthboundTest):
    def setUp(self):
        super(TestLogicalRouterPortOps, self).setUp()
        self.lr = self.useFixture(fixtures.LogicalRouterFixture(self.api)).obj

    def _lrp_add(self, port, mac='de:ad:be:ef:4d:ad',
                 networks=None, *args, **kwargs):
        if port is None:
            port = utils.get_rand_device_name()
        if networks is None:
            networks = ['192.0.2.0/24']
        lrp = self.api.lrp_add(self.lr.uuid, port, mac, networks,
                               *args, **kwargs).execute(check_error=True)
        self.assertIn(lrp, self.lr.ports)
        self.assertEqual(mac, lrp.mac)
        self.assertEqual(set(networks), set(lrp.networks))
        return lrp

    def _test_lrp_get(self, col):
        lrp = self._lrp_add(None)
        val = getattr(lrp, col)
        found = self.api.lrp_get(val).execute(check_error=True)
        self.assertEqual(lrp, found)

    def test_lrp_get_uuid(self):
        self._test_lrp_get('uuid')

    def test_lrp_get_name(self):
        self._test_lrp_get('name')

    def test_lrp_add(self):
        self._lrp_add(None, 'de:ad:be:ef:4d:ad', ['192.0.2.0/24'])

    def test_lrp_add_peer(self):
        lrp = self._lrp_add(None, 'de:ad:be:ef:4d:ad', ['192.0.2.0/24'],
                            peer='fake_peer')
        self.assertIn('fake_peer', lrp.peer)

    def test_lpr_add_multiple_networks(self):
        networks = ['192.0.2.0/24', '192.2.1.0/24']
        self._lrp_add(None, 'de:ad:be:ef:4d:ad', networks)

    def test_lrp_add_invalid_mac(self):
        self.assertRaises(
            netaddr.AddrFormatError,
            self.api.lrp_add, "fake", "fake", "000:11:22:33:44:55",
            ['192.0.2.0/24'])

    def test_lrp_add_invalid_network(self):
        self.assertRaises(
            netaddr.AddrFormatError,
            self.api.lrp_add, "fake", "fake", "01:02:03:04:05:06",
            ['256.2.0.1/24'])

    def test_lrp_add_exists(self):
        name = utils.get_rand_device_name()
        args = (name, 'de:ad:be:ef:4d:ad', ['192.0.2.0/24'])
        self._lrp_add(*args)
        self.assertRaises(RuntimeError, self._lrp_add, *args)

    def test_lrp_add_may_exist(self):
        name = utils.get_rand_device_name()
        args = (name, 'de:ad:be:ef:4d:ad', ['192.0.2.0/24'])
        self._lrp_add(*args)
        self._lrp_add(*args, may_exist=True)

    def test_lrp_add_may_exist_different_router(self):
        name = utils.get_rand_device_name()
        args = (name, 'de:ad:be:ef:4d:ad', ['192.0.2.0/24'])
        lr2 = self.useFixture(fixtures.LogicalRouterFixture(self.api)).obj
        self._lrp_add(*args)
        cmd = self.api.lrp_add(lr2.uuid, *args, may_exist=True)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lrp_add_may_exist_different_mac(self):
        name = utils.get_rand_device_name()
        args = {'port': name, 'mac': 'de:ad:be:ef:4d:ad',
                'networks': ['192.0.2.0/24']}
        self._lrp_add(**args)
        args['mac'] = 'da:d4:de:ad:be:ef'
        self.assertRaises(RuntimeError, self._lrp_add, may_exist=True, **args)

    def test_lrp_add_may_exist_different_networks(self):
        name = utils.get_rand_device_name()
        args = (name, 'de:ad:be:ef:4d:ad')
        self._lrp_add(*args, networks=['192.0.2.0/24'])
        self.assertRaises(RuntimeError, self._lrp_add, *args,
                          networks=['192.2.1.0/24'], may_exist=True)

    def test_lrp_add_may_exist_different_peer(self):
        name = utils.get_rand_device_name()
        args = (name, 'de:ad:be:ef:4d:ad', ['192.0.2.0/24'])
        self._lrp_add(*args)
        self.assertRaises(RuntimeError, self._lrp_add, *args,
                          peer='fake', may_exist=True)

    def test_lrp_add_columns(self):
        options = {'myside': 'yourside'}
        external_ids = {'myside': 'yourside'}
        lrp = self._lrp_add(None, options=options, external_ids=external_ids)
        self.assertEqual(options, lrp.options)
        self.assertEqual(external_ids, lrp.external_ids)

    def test_lrp_add_gw_chassis(self):
        name, c1, c2 = [utils.get_rand_device_name() for _ in range(3)]
        args = (name, 'de:ad:be:ef:4d:ad')
        lrp = self._lrp_add(*args, gateway_chassis=(c1, c2))
        c1 = self.api.lookup('Gateway_Chassis', "%s_%s" % (lrp.name, c1))
        c2 = self.api.lookup('Gateway_Chassis', "%s_%s" % (lrp.name, c2))
        self.assertIn(c1, lrp.gateway_chassis)
        self.assertIn(c2, lrp.gateway_chassis)

    def test_gwc_add(self):
        # NOTE: no API method to create gateway chassis directly
        name, c1_name = [utils.get_rand_device_name() for _ in range(2)]
        lrp = self._lrp_add(name, gateway_chassis=[c1_name])
        c1 = self.api.lookup('Gateway_Chassis', "%s_%s" % (lrp.name, c1_name))
        self.assertEqual(c1.name, "%s_%s" % (lrp.name, c1_name))
        self.assertEqual(c1.chassis_name, c1_name)
        self.assertEqual(c1.priority, 1)

    def test_lrp_del_uuid(self):
        lrp = self._lrp_add(None)
        self.api.lrp_del(lrp.uuid).execute(check_error=True)
        self.assertNotIn(lrp, self.lr.ports)

    def test_lrp_del_name(self):
        lrp = self._lrp_add(None)
        self.api.lrp_del(lrp.name).execute(check_error=True)
        self.assertNotIn(lrp, self.lr.ports)

    def test_lrp_del_router(self):
        lrp = self._lrp_add(None)
        self.api.lrp_del(lrp.uuid, self.lr.uuid).execute(check_error=True)
        self.assertNotIn(lrp, self.lr.ports)

    def test_lrp_del_router_name(self):
        lrp = self._lrp_add(None)
        self.api.lrp_del(lrp.uuid,
                         self.lr.name).execute(check_error=True)
        self.assertNotIn(lrp, self.lr.ports)

    def test_lrp_del_wrong_router(self):
        lrp = self._lrp_add(None)
        sw = self.useFixture(fixtures.LogicalSwitchFixture(self.api)).obj
        cmd = self.api.lrp_del(lrp.uuid, sw.uuid)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lrp_del_router_no_exist(self):
        lrp = self._lrp_add(None)
        cmd = self.api.lrp_del(lrp.uuid, utils.get_rand_device_name())
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lrp_del_no_exist(self):
        cmd = self.api.lrp_del("fake_port")
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lrp_del_if_exist(self):
        self.api.lrp_del("fake_port", if_exists=True).execute(check_error=True)

    def test_lrp_list(self):
        ports = {self._lrp_add(None) for _ in range(3)}
        port_set = set(self.api.lrp_list(self.lr.uuid).execute(
            check_error=True))
        self.assertTrue(ports.issubset(port_set))

    def test_lrp_get_set_enabled(self):
        lrp = self._lrp_add(None)
        # default is True
        self.assertTrue(self.api.lrp_get_enabled(lrp.name).execute(
            check_error=True))
        self.api.lrp_set_enabled(lrp.name, False).execute(check_error=True)
        self.assertFalse(self.api.lrp_get_enabled(lrp.name).execute(
            check_error=True))
        self.api.lrp_set_enabled(lrp.name, True).execute(check_error=True)
        self.assertTrue(self.api.lrp_get_enabled(lrp.name).execute(
            check_error=True))

    def test_lrp_get_set_options(self):
        options = {'one': 'two', 'three': 'four'}
        lrp = self._lrp_add(None)
        self.api.lrp_set_options(lrp.uuid, **options).execute(
            check_error=True)
        self.assertEqual(options, self.api.lrp_get_options(lrp.uuid).execute(
            check_error=True))

    def test_lrp_set_options_if_exists(self):
        options = {'one': 'two', 'three': 'four'}
        self.api.lrp_set_options(utils.get_rand_device_name(),
                                 if_exists=True,
                                 **options).execute(check_error=True)

    def test_lrp_set_options_no_exist(self):
        options = {'one': 'two', 'three': 'four'}
        cmd = self.api.lrp_set_options(utils.get_rand_device_name(), **options)
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_lrp_get_set_gateway_chassis(self):
        lrp = self._lrp_add(None)
        c1_name = utils.get_rand_device_name()
        self.api.lrp_set_gateway_chassis(lrp.uuid, c1_name).execute(
            check_error=True)
        c1 = self.api.lookup('Gateway_Chassis', "%s_%s" % (lrp.name, c1_name))
        lrp_gwcs = self.api.lrp_get_gateway_chassis(lrp.uuid).execute(
            check_error=True)
        self.assertIn(c1, lrp_gwcs)
        self.assertEqual(c1.name, "%s_%s" % (lrp.name, c1_name))
        self.assertEqual(c1.chassis_name, c1_name)
        self.assertEqual(c1.priority, 0)

    def test_lrp_set_multiple_gwc(self):
        lrp = self._lrp_add(None)
        c1_name, c2_name = [utils.get_rand_device_name() for _ in range(2)]
        for gwc in [c1_name, c2_name]:
            self.api.lrp_set_gateway_chassis(lrp.uuid, gwc).execute(
                check_error=True)
        c1 = self.api.lookup('Gateway_Chassis', "%s_%s" % (lrp.name, c1_name))
        c2 = self.api.lookup('Gateway_Chassis', "%s_%s" % (lrp.name, c2_name))
        lrp_gwcs = self.api.lrp_get_gateway_chassis(lrp.uuid).execute(
            check_error=True)
        self.assertIn(c1, lrp_gwcs)
        self.assertIn(c2, lrp_gwcs)

    def test_lrp_del_gateway_chassis(self):
        c1_name = utils.get_rand_device_name()
        lrp = self._lrp_add(None, gateway_chassis=[c1_name])
        c1 = self.api.lookup('Gateway_Chassis', "%s_%s" % (lrp.name, c1_name))
        self.assertEqual(lrp.gateway_chassis, [c1])
        self.api.lrp_del_gateway_chassis(lrp.uuid, c1_name).execute(
            check_error=True)
        self.assertEqual(lrp.gateway_chassis, [])
        self.assertRaises(idlutils.RowNotFound,
                          self.api.lookup,
                          'Gateway_Chassis', "%s_%s" % (lrp.name, c1_name))

    def test_lrp_del_gateway_chassis_wrong_chassis(self):
        lrp = self._lrp_add(None)
        cmd = self.api.lrp_del_gateway_chassis(lrp.uuid, "fake_chassis")
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lrp_del_gateway_chassis_if_exist(self):
        lrp = self._lrp_add(None)
        self.api.lrp_del_gateway_chassis(
            lrp.uuid, "fake_chassis", if_exists=True
        ).execute(check_error=True)

    def test_lrp_add_del_network(self):
        networks = ['10.0.0.0/24']
        new_networks = ['172.31.0.0/24', '172.31.1.0/24']
        lrp = self._lrp_add(None, networks=networks)

        self.api.lrp_add_networks(lrp.name,
                                  new_networks).execute(check_error=True)
        self.assertEqual(lrp.networks, networks + new_networks)

        self.api.lrp_del_networks(lrp.name,
                                  new_networks).execute(check_error=True)
        self.assertEqual(lrp.networks, networks)

    def test_lrp_add_del_network_by_str(self):
        networks = ['10.0.0.0/24']
        new_network = '172.31.0.0/24'
        lrp = self._lrp_add(None, networks=networks)

        self.api.lrp_add_networks(lrp.name,
                                  new_network).execute(check_error=True)
        self.assertEqual(lrp.networks, networks + [new_network])

        self.api.lrp_del_networks(lrp.name,
                                  new_network).execute(check_error=True)
        self.assertEqual(lrp.networks, networks)

    def test_lrp_add_del_network_negative(self):
        networks = ['10.0.0.0/24']
        no_existing_network = '192.168.0.0/24'
        lrp = self._lrp_add(None, networks=networks)

        cmd = self.api.lrp_add_networks(lrp.name, networks)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

        cmd = self.api.lrp_del_networks(lrp.name, no_existing_network)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

        for new_net in ["fake", ["10.0.1.0/24", "fake"]]:
            self.assertRaises(netaddr.AddrFormatError,
                              self.api.lrp_add_networks, lrp.name, new_net)
            self.assertEqual(lrp.networks, networks)

            self.assertRaises(netaddr.AddrFormatError,
                              self.api.lrp_del_networks, lrp.name, new_net)
            self.assertEqual(lrp.networks, networks)


class TestLoadBalancerOps(OvnNorthboundTest):

    def _lb_add(self, lb, vip, ips, protocol=const.PROTO_TCP, may_exist=False,
                **columns):
        lbal = self.useFixture(fixtures.LoadBalancerFixture(
            self.api, lb, vip, ips, protocol, may_exist, **columns)).obj
        self.assertEqual(lb, lbal.name)
        norm_vip = ovsdb_utils.normalize_ip_port(vip)
        self.assertIn(norm_vip, lbal.vips)
        self.assertEqual(",".join(ovsdb_utils.normalize_ip(ip) for ip in ips),
                         lbal.vips[norm_vip])
        self.assertIn(protocol, lbal.protocol)  # because optional
        return lbal

    def test_lb_add(self):
        vip = '192.0.2.1'
        ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3']
        self._lb_add(utils.get_rand_device_name(), vip, ips)

    def test_lb_add_port(self):
        vip = '192.0.2.1:80'
        ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3']
        self._lb_add(utils.get_rand_device_name(), vip, ips)

    def test_lb_add_protocol(self):
        vip = '192.0.2.1'
        ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3']
        self._lb_add(utils.get_rand_device_name(), vip, ips, const.PROTO_UDP)

    def test_lb_add_new_vip(self):
        name = utils.get_rand_device_name()
        lb1 = self._lb_add(name, '192.0.2.1', ['10.0.0.1', '10.0.0.2'])
        lb2 = self._lb_add(name, '192.0.2.2', ['10.1.0.1', '10.1.0.2'])
        self.assertEqual(lb1, lb2)
        self.assertEqual(2, len(lb1.vips))

    def test_lb_add_exists(self):
        name = utils.get_rand_device_name()
        vip = '192.0.2.1'
        ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3']
        self._lb_add(name, vip, ips)
        cmd = self.api.lb_add(name, vip, ips)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_lb_add_may_exist(self):
        name = utils.get_rand_device_name()
        vip = '192.0.2.1'
        ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3']
        lb1 = self._lb_add(name, vip, ips)
        ips += ['10.0.0.4']
        lb2 = self.api.lb_add(name, vip, ips, may_exist=True).execute(
            check_error=True)
        self.assertEqual(lb1, lb2)
        self.assertEqual(",".join(ips), lb1.vips[vip])

    def test_lb_add_columns(self):
        ext_ids = {'one': 'two'}
        name = utils.get_rand_device_name()
        lb = self._lb_add(name, '192.0.2.1', ['10.0.0.1', '10.0.0.2'],
                          external_ids=ext_ids)
        self.assertEqual(ext_ids, lb.external_ids)

    def test_lb_del(self):
        name = utils.get_rand_device_name()
        lb = self._lb_add(name, '192.0.2.1', ['10.0.0.1', '10.0.0.2']).uuid
        self.api.lb_del(lb).execute(check_error=True)
        self.assertNotIn(lb, self.api.tables['Load_Balancer'].rows)

    def test_lb_del_vip(self):
        name = utils.get_rand_device_name()
        lb1 = self._lb_add(name, '192.0.2.1', ['10.0.0.1', '10.0.0.2'])
        lb2 = self._lb_add(name, '192.0.2.2', ['10.1.0.1', '10.1.0.2'])
        self.assertEqual(lb1, lb2)
        self.api.lb_del(lb1.name, '192.0.2.1').execute(check_error=True)
        self.assertNotIn('192.0.2.1', lb1.vips)
        self.assertIn('192.0.2.2', lb1.vips)

    def test_lb_del_no_exist(self):
        cmd = self.api.lb_del(utils.get_rand_device_name())
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_lb_del_if_exists(self):
        self.api.lb_del(utils.get_rand_device_name(), if_exists=True).execute(
            check_error=True)

    def test_lb_del_vip_if_exists(self):
        name = utils.get_rand_device_name()
        lb = self._lb_add(name, '192.0.2.1', ['10.0.0.1'])
        lb2 = self._lb_add(name, '192.0.2.2', ['10.1.0.1'])
        self.assertEqual(lb, lb2)
        # Remove vip that does not exist.
        self.api.lb_del(lb.name, '9.9.9.9', if_exists=True
                        ).execute(check_error=True)
        self.assertIn('192.0.2.1', lb.vips)
        self.assertIn('192.0.2.2', lb.vips)
        # Remove one vip.
        self.api.lb_del(lb.name, '192.0.2.1', if_exists=True
                        ).execute(check_error=True)
        self.assertNotIn('192.0.2.1', lb.vips)
        # Attempt to remove same vip a second time with if_exists=True.
        self.api.lb_del(lb.name, '192.0.2.1', if_exists=True).execute(
            check_error=True)
        # Attempt to remove same vip a third time with if_exists=False.
        cmd = self.api.lb_del(lb.name, '192.0.2.1', if_exists=False)
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)
        # Remove last vip and make sure that lb is also removed.
        self.assertIn('192.0.2.2', lb.vips)
        self.api.lb_del(lb.name, '192.0.2.2').execute(check_error=True)
        cmd = self.api.lb_del(lb.name)
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_lb_list(self):
        lbs = {self._lb_add(utils.get_rand_device_name(), '192.0.2.1',
                            ['10.0.0.1', '10.0.0.2']) for _ in range(3)}
        lbset = self.api.lb_list().execute(check_error=True)
        self.assertTrue(lbs.issubset(lbset))

    def _test_lb_get(self, col):
        lb = self._lb_add(utils.get_rand_device_name(),
                          '192.0.0.1', ['10.0.0.1'])
        val = getattr(lb, col)
        found = self.api.lb_get(val).execute(check_error=True)
        self.assertEqual(lb, found)

    def test_lb_get_uuid(self):
        self._test_lb_get('uuid')

    def test_lb_get_name(self):
        self._test_lb_get('name')

    def _test_lb_add_del_health_check(self, col):
        hc_options = {
            'interval': '2',
            'timeout': '10',
            'success_count': '3',
            'failure_count': '3',
        }
        hc_vip = '172.31.0.1'

        lb = self._lb_add(utils.get_rand_device_name(),
                          '192.0.0.1', ['10.0.0.1'])
        self.assertEqual(lb.health_check, [])
        val = getattr(lb, col)
        self.api.lb_add_health_check(val,
                                     hc_vip,
                                     **hc_options).execute(check_error=True)
        self.assertEqual(len(lb.health_check), 1)
        hc = self.api.lookup('Load_Balancer_Health_Check',
                             lb.health_check[0].uuid)
        self.assertEqual(hc.vip, hc_vip)
        self.assertEqual(hc.options, hc_options)

        self.api.lb_del_health_check(val, hc.uuid).execute(check_error=True)
        self.assertEqual(len(lb.health_check), 0)
        self.assertNotIn(hc.uuid,
                         self.api.tables['Load_Balancer_Health_Check'].rows)

    def test_lb_add_del_health_check_uuid(self):
        self._test_lb_add_del_health_check('uuid')

    def test_lb_add_del_health_check_name(self):
        self._test_lb_add_del_health_check('name')

    def test_lb_del_health_check_if_exists(self):
        lb = self._lb_add(utils.get_rand_device_name(),
                          '192.0.0.1', ['10.0.0.1'])
        self.api.lb_del_health_check(lb.name, uuid.uuid4(),
                                     if_exists=True).execute(check_error=True)

    def _test_lb_add_del_ip_port_mapping(self, col, input, expected):
        endpoint_ip, source_ip = input
        expected_endpoint_ip, expected_source_ip = expected
        port_name = 'sw1-p1'

        lb = self._lb_add(utils.get_rand_device_name(),
                          '192.0.0.1', ['10.0.0.1'])
        self.assertEqual(lb.ip_port_mappings, {})
        val = getattr(lb, col)
        self.api.lb_add_ip_port_mapping(val,
                                        endpoint_ip,
                                        port_name,
                                        source_ip).execute(check_error=True)
        self.assertEqual(lb.ip_port_mappings[expected_endpoint_ip],
                         '%s:%s' % (port_name, expected_source_ip))

        self.api.lb_del_ip_port_mapping(val,
                                        endpoint_ip).execute(check_error=True)
        self.assertEqual(lb.ip_port_mappings, {})

    def test_lb_add_del_ip_port_mapping_uuid(self):
        input = ('172.31.0.3', '172.31.0.6')
        self._test_lb_add_del_ip_port_mapping('uuid', input, input)

    def test_lb_add_del_ip_port_mapping_uuid_v6(self):
        input = ('2001:db8::1', '2001:db8::2')
        expected = (f"[{input[0]}]", f"[{input[1]}]")
        self._test_lb_add_del_ip_port_mapping('uuid', input, expected)

    def test_lb_add_del_ip_port_mapping_name(self):
        input = ('172.31.0.3', '172.31.0.6')
        self._test_lb_add_del_ip_port_mapping('name', input, input)

    def test_lb_add_del_ip_port_mapping_name_v6(self):
        input = ('2001:db8::1', '2001:db8::2')
        expected = (f"[{input[0]}]", f"[{input[1]}]")
        self._test_lb_add_del_ip_port_mapping('name', input, expected)

    def test_hc_get_set_options(self):
        hc_options = {
            'interval': '2',
            'timeout': '10',
            'success_count': '3',
            'failure_count': '3',
        }
        lb = self._lb_add(utils.get_rand_device_name(),
                          '192.0.0.1', ['10.0.0.1'])
        self.api.lb_add_health_check(lb.uuid,
                                     '172.31.0.1',
                                     **hc_options).execute(check_error=True)
        hc = self.api.lookup('Load_Balancer_Health_Check',
                             lb.health_check[0].uuid)
        options = self.api.health_check_get_options(
            hc.uuid).execute(check_error=True)
        self.assertEqual(hc_options, options)

        options.update({
            'interval': '5',
            'new-option': 'option',
        })
        self.api.health_check_set_options(hc.uuid,
                                          **options).execute(check_error=True)
        self.assertEqual(hc.options, options)


class TestObLbOps(testscenarios.TestWithScenarios, OvnNorthboundTest):
    scenarios = [
        ('LrLbOps', dict(fixture=fixtures.LogicalRouterFixture,
                         _add_fn='lr_lb_add', _del_fn='lr_lb_del',
                         _list_fn='lr_lb_list')),
        ('LsLbOps', dict(fixture=fixtures.LogicalSwitchFixture,
                         _add_fn='ls_lb_add', _del_fn='ls_lb_del',
                         _list_fn='ls_lb_list')),
    ]

    def setUp(self):
        super(TestObLbOps, self).setUp()
        self.add_fn = getattr(self.api, self._add_fn)
        self.del_fn = getattr(self.api, self._del_fn)
        self.list_fn = getattr(self.api, self._list_fn)
        # They must be in this order because the load balancer
        # can't be deleted when there is a reference in the router
        self.lb = self.useFixture(fixtures.LoadBalancerFixture(
            self.api, utils.get_rand_device_name(), '192.0.2.1',
            ['10.0.0.1', '10.0.0.2'])).obj
        self.lb2 = self.useFixture(fixtures.LoadBalancerFixture(
            self.api, utils.get_rand_device_name(), '192.0.2.2',
            ['10.1.0.1', '10.1.0.2'])).obj
        self.lr = self.useFixture(self.fixture(
            self.api, utils.get_rand_device_name())).obj

    def test_ob_lb_add(self):
        self.add_fn(self.lr.name, self.lb.name).execute(
            check_error=True)
        self.assertIn(self.lb, self.lr.load_balancer)

    def test_ob_lb_add_exists(self):
        cmd = self.add_fn(self.lr.name, self.lb.name)
        cmd.execute(check_error=True)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_ob_lb_add_may_exist(self):
        cmd = self.add_fn(self.lr.name, self.lb.name, may_exist=True)
        lb1 = cmd.execute(check_error=True)
        lb2 = cmd.execute(check_error=True)
        self.assertEqual(lb1, lb2)

    def test_ob_lb_del(self):
        self.add_fn(self.lr.name, self.lb.name).execute(
            check_error=True)
        self.assertIn(self.lb, self.lr.load_balancer)
        self.del_fn(self.lr.name).execute(check_error=True)
        self.assertEqual(0, len(self.lr.load_balancer))

    def test_ob_lb_del_lb(self):
        self.add_fn(self.lr.name, self.lb.name).execute(
            check_error=True)
        self.add_fn(self.lr.name, self.lb2.name).execute(
            check_error=True)
        self.del_fn(self.lr.name, self.lb2.name).execute(
            check_error=True)
        self.assertNotIn(self.lb2, self.lr.load_balancer)
        self.assertIn(self.lb, self.lr.load_balancer)

    def test_ob_lb_del_no_exist(self):
        cmd = self.del_fn(self.lr.name, 'fake')
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_ob_lb_del_if_exists(self):
        self.del_fn(self.lr.name, 'fake', if_exists=True).execute(
            check_error=True)

    def test_ob_lb_list(self):
        self.add_fn(self.lr.name, self.lb.name).execute(
            check_error=True)
        self.add_fn(self.lr.name, self.lb2.name).execute(
            check_error=True)
        rows = self.list_fn(self.lr.name).execute(check_error=True)
        self.assertIn(self.lb, rows)
        self.assertIn(self.lb2, rows)


class TestCommonDbOps(OvnNorthboundTest):
    def setUp(self):
        super(TestCommonDbOps, self).setUp()
        name = utils.get_rand_device_name()
        self.switch = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api, name)).obj
        self.lsps = [
            self.api.lsp_add(
                self.switch.uuid,
                utils.get_rand_device_name()).execute(check_error=True)
            for _ in range(3)]
        self.api.db_set('Logical_Switch', self.switch.uuid,
                        ('external_ids', {'one': '1', 'two': '2'})).execute(
                            check_error=True)

    def _ls_get_extids(self):
        return self.api.db_get('Logical_Switch', self.switch.uuid,
                               'external_ids').execute(check_error=True)

    def test_db_remove_map_key(self):
        ext_ids = self._ls_get_extids()
        removed = ext_ids.popitem()
        self.api.db_remove('Logical_Switch', self.switch.uuid,
                           'external_ids', removed[0]).execute(
            check_error=True)
        self.assertEqual(ext_ids, self.switch.external_ids)

    def test_db_remove_map_value(self):
        ext_ids = self._ls_get_extids()
        removed = dict([ext_ids.popitem()])
        self.api.db_remove('Logical_Switch', self.switch.uuid,
                           'external_ids', **removed).execute(
            check_error=True)
        self.assertEqual(ext_ids, self.switch.external_ids)

    def test_db_remove_map_bad_key(self):
        # should be a NoOp, not fail
        self.api.db_remove('Logical_Switch', self.switch.uuid,
                           'external_ids', "badkey").execute(check_error=True)

    def test_db_remove_map_bad_value(self):
        ext_ids = self._ls_get_extids()
        removed = {ext_ids.popitem()[0]: "badvalue"}
        # should be a NoOp, not fail
        self.api.db_remove('Logical_Switch', self.switch.uuid,
                           'external_ids', **removed).execute(check_error=True)

    def test_db_remove_value(self):
        ports = self.api.db_get('Logical_Switch', self.switch.uuid,
                                'ports').execute(check_error=True)
        removed = ports.pop()
        self.api.db_remove('Logical_Switch', self.switch.uuid, 'ports',
                           removed).execute(check_error=True)
        self.assertEqual(ports, [x.uuid for x in self.switch.ports])

    def test_db_remove_bad_value(self):
        # should be a NoOp, not fail
        self.api.db_remove('Logical_Switch', self.switch.uuid, 'ports',
                           "badvalue").execute(check_error=True)


class TestDnsOps(OvnNorthboundTest):
    def _dns_add(self, *args, **kwargs):
        dns = self.useFixture(
            fixtures.DnsFixture(self.api, *args, **kwargs)).obj
        return dns

    def test_dns_get(self):
        dns = self._dns_add()
        found = self.api.dns_get(dns.uuid).execute(
            check_error=True)
        self.assertEqual(dns, found)

    def test_dns_get_no_exist(self):
        cmd = self.api.dns_get("noexist")
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_dns_add(self):
        self._dns_add()

    def test_dns_add_ext_ids(self):
        ext_ids = {'net-id': '1', 'other-id': '2'}
        dns = self._dns_add(external_ids=ext_ids)
        self.assertEqual(ext_ids, dns.external_ids)

    def test_dns_list(self):
        dnses = {self._dns_add() for d in range(3)}
        dnses_set = set(
            self.api.dns_list().execute(check_error=True))
        self.assertTrue(dnses.issubset(dnses_set))

    def test_dns_set_records(self):
        dns = self._dns_add()
        records = {'a': 'one', 'b': 'two'}
        self.api.dns_set_records(
            dns.uuid, **records).execute(check_error=True)
        dns = self.api.dns_get(dns.uuid).execute(
            check_error=True)
        self.assertEqual(records, dns.records)
        self.api.dns_set_records(
            dns.uuid, **{}).execute(check_error=True)
        self.assertEqual({}, dns.records)

    def test_dns_set_external_ids(self):
        dns = self._dns_add()
        external_ids = {'a': 'one', 'b': 'two'}
        self.api.dns_set_external_ids(
            dns.uuid, **external_ids).execute(check_error=True)
        dns = self.api.dns_get(dns.uuid).execute(
            check_error=True)
        self.assertEqual(external_ids, dns.external_ids)
        self.api.dns_set_external_ids(
            dns.uuid, **{}).execute(check_error=True)
        self.assertEqual({}, dns.external_ids)

    def test_dns_set_options(self):
        dns = self._dns_add()
        options = {'a': 'one', 'b': 'two'}
        self.api.dns_set_options(
            dns.uuid, **options).execute(check_error=True)
        dns = self.api.dns_get(dns.uuid).execute(
            check_error=True)
        self.assertEqual(options, dns.options)
        self.api.dns_set_options(
            dns.uuid, **{}).execute(check_error=True)
        self.assertEqual({}, dns.options)

    def test_dns_set_options_if_exists(self):
        non_existent_dns = ovsdb_utils.generate_uuid()
        options = {}

        # Assert that if if_exists is True (default ) it won't raise an error
        self.api.dns_set_options(
            non_existent_dns, **options).execute(check_error=True)

        # Assert that if if_exists is False it will raise an error
        self.assertRaises(RuntimeError, self.api.dns_set_options(
            non_existent_dns, if_exists=False, **options).execute, True)

    def test_dns_add_remove_records(self):
        dns = self._dns_add()
        self.api.dns_add_record(dns.uuid, 'a', 'one').execute()
        self.api.dns_add_record(dns.uuid, 'b', 'two').execute()
        dns = self.api.dns_get(dns.uuid).execute(
            check_error=True)
        records = {'a': 'one', 'b': 'two'}
        self.assertEqual(records, dns.records)
        self.api.dns_remove_record(dns.uuid, 'a').execute()
        records.pop('a')
        self.assertEqual(records, dns.records)
        self.api.dns_remove_record(dns.uuid, 'b').execute()
        self.assertEqual({}, dns.records)


class TestLsDnsOps(OvnNorthboundTest):
    def _dns_add(self, *args, **kwargs):
        dns = self.useFixture(
            fixtures.DnsFixture(self.api, *args, **kwargs)).obj
        return dns

    def _ls_add(self, *args, **kwargs):
        fix = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api, *args, **kwargs))
        return fix.obj

    def test_ls_dns_set_clear_records(self):
        dns1 = self._dns_add()
        dns2 = self._dns_add()

        ls1 = self._ls_add('ls1')
        self.api.ls_set_dns_records(ls1.uuid, [dns1.uuid, dns2.uuid]).execute()
        self.assertCountEqual([dns1.uuid, dns2.uuid],
                              [dns.uuid for dns in ls1.dns_records])

        self.api.ls_clear_dns_records(ls1.uuid).execute()
        self.assertEqual([], ls1.dns_records)

    def test_ls_dns_add_remove_records(self):
        dns1 = self._dns_add()
        dns2 = self._dns_add()

        ls1 = self._ls_add('ls1')

        self.api.ls_add_dns_record(ls1.uuid, dns1.uuid).execute()
        self.assertCountEqual([dns1.uuid],
                              [dns.uuid for dns in ls1.dns_records])

        self.api.ls_add_dns_record(ls1.uuid, dns2.uuid).execute()
        self.assertCountEqual([dns1.uuid, dns2.uuid],
                              [dns.uuid for dns in ls1.dns_records])

        self.api.ls_remove_dns_record(ls1.uuid, dns2.uuid).execute()
        self.assertCountEqual([dns1.uuid],
                              [dns.uuid for dns in ls1.dns_records])
        self.api.ls_remove_dns_record(ls1.uuid, dns1.uuid).execute()
        self.assertEqual([], ls1.dns_records)


class TestPortGroup(OvnNorthboundTest):

    def setUp(self):
        super(TestPortGroup, self).setUp()
        self.switch = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api)).obj
        self.pg_name = 'testpg-%s' % ovsdb_utils.generate_uuid()

    def test_port_group(self):
        # Assert the Port Group was added
        self.api.pg_add(self.pg_name).execute(check_error=True)
        row = self.api.db_find(
            'Port_Group',
            ('name', '=', self.pg_name)).execute(check_error=True)
        self.assertIsNotNone(row)
        self.assertEqual(self.pg_name, row[0]['name'])
        self.assertEqual([], row[0]['ports'])
        self.assertEqual([], row[0]['acls'])

        # Assert the Port Group was deleted
        self.api.pg_del(self.pg_name).execute(check_error=True)
        row = self.api.db_find(
            'Port_Group',
            ('name', '=', self.pg_name)).execute(check_error=True)
        self.assertEqual([], row)

    def test_port_group_ports(self):
        lsp_add_cmd = self.api.lsp_add(self.switch.uuid, 'testport')
        with self.api.transaction(check_error=True) as txn:
            txn.add(lsp_add_cmd)
            txn.add(self.api.pg_add(self.pg_name))

        port_uuid = lsp_add_cmd.result.uuid

        # Lets add the port using the UUID instead of a `Command` to
        # exercise the API
        self.api.pg_add_ports(self.pg_name, port_uuid).execute(
            check_error=True)
        row = self.api.db_find(
            'Port_Group',
            ('name', '=', self.pg_name)).execute(check_error=True)
        self.assertIsNotNone(row)
        self.assertEqual(self.pg_name, row[0]['name'])
        # Assert the port was added from the Port Group
        self.assertEqual([port_uuid], row[0]['ports'])

        # Delete the Port from the Port Group
        with self.api.transaction(check_error=True) as txn:
            txn.add(self.api.pg_del_ports(self.pg_name, port_uuid))

        row = self.api.db_find(
            'Port_Group',
            ('name', '=', self.pg_name)).execute(check_error=True)
        self.assertIsNotNone(row)
        self.assertEqual(self.pg_name, row[0]['name'])
        # Assert the port was removed from the Port Group
        self.assertEqual([], row[0]['ports'])

    def test_pg_del_ports_if_exists(self):
        self.api.pg_add(self.pg_name).execute(check_error=True)
        non_existent_res = ovsdb_utils.generate_uuid()

        # Assert that if if_exists is False (default) it will raise an error
        self.assertRaises(RuntimeError, self.api.pg_del_ports(self.pg_name,
                          non_existent_res).execute, True)

        # Assert that if if_exists is True it won't raise an error
        self.api.pg_del_ports(self.pg_name, non_existent_res,
                              if_exists=True).execute(check_error=True)


class TestHAChassisGroup(OvnNorthboundTest):

    def setUp(self):
        super(TestHAChassisGroup, self).setUp()
        self.hcg_name = 'ha-group-%s' % ovsdb_utils.generate_uuid()
        self.chassis = 'chassis-%s' % ovsdb_utils.generate_uuid()

    def test_ha_chassis_group(self):
        # Assert the HA Chassis Group was added
        self.api.ha_chassis_group_add(self.hcg_name).execute(check_error=True)
        hcg = self.api.ha_chassis_group_get(self.hcg_name).execute(
            check_error=True)
        self.assertEqual(self.hcg_name, hcg.name)

        # Assert the HA Chassis Group was deleted
        self.api.ha_chassis_group_del(self.hcg_name).execute(check_error=True)
        cmd = self.api.ha_chassis_group_get(self.hcg_name)
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

    def test_ha_chassis_group_add_delete_chassis(self):
        self.api.ha_chassis_group_add(self.hcg_name).execute(check_error=True)
        priority = 20
        self.api.ha_chassis_group_add_chassis(
            self.hcg_name, self.chassis, priority).execute(check_error=True)

        # Assert that the HA Chassis entry was created
        row = self.api.db_find(
            'HA_Chassis',
            ('chassis_name', '=', self.chassis)).execute(check_error=True)
        self.assertEqual(priority, row[0]['priority'])

        # Assert that the HA Chassis entry was associated with
        # the HA Chassis Group
        hcg = self.api.ha_chassis_group_get(self.hcg_name).execute(
            check_error=True)
        self.assertEqual(self.chassis, hcg.ha_chassis[0].chassis_name)

        # Deletes the HA Chassis entry
        self.api.ha_chassis_group_del_chassis(
            self.hcg_name, self.chassis).execute(check_error=True)
        row = self.api.db_find(
            'HA_Chassis',
            ('chassis_name', '=', self.chassis)).execute(check_error=True)
        self.assertEqual([], row)

        # Assert that the deleted HA Chassis entry was dissociated from
        # the HA Chassis Group
        hcg = self.api.ha_chassis_group_get(self.hcg_name).execute(
            check_error=True)
        self.assertEqual([], hcg.ha_chassis)

    def test_ha_chassis_group_add_delete_chassis_within_txn(self):
        with self.api.create_transaction(check_error=True) as txn:
            hcg_cmd = txn.add(self.api.ha_chassis_group_add(self.hcg_name))
            priority = 20
            txn.add(self.api.ha_chassis_group_add_chassis(
                hcg_cmd, self.chassis, priority))

        # Assert that the HA Chassis entry was created
        hcg = self.api.ha_chassis_group_get(self.hcg_name).execute(
            check_error=True)
        hc = self.api.db_find(
            'HA_Chassis',
            ('chassis_name', '=', self.chassis)).execute(check_error=True)
        self.assertEqual(priority, hc[0]['priority'])
        ha_chassis_uuid_list = [hc.uuid for hc in hcg.ha_chassis]
        self.assertEqual(ha_chassis_uuid_list, [hc[0]['_uuid']])

        with self.api.create_transaction(check_error=True) as txn:
            hcg_cmd = txn.add(self.api.ha_chassis_group_add(self.hcg_name,
                                                            may_exist=True))
            txn.add(self.api.ha_chassis_group_del_chassis(hcg_cmd,
                                                          self.chassis))

        hcg = self.api.ha_chassis_group_get(self.hcg_name).execute(
            check_error=True)
        ha = self.api.db_find(
            'HA_Chassis',
            ('chassis_name', '=', self.chassis)).execute(check_error=True)
        self.assertEqual([], ha)
        self.assertEqual([], hcg.ha_chassis)

    def test_ha_chassis_group_if_exists(self):
        self.api.ha_chassis_group_add(self.hcg_name).execute(check_error=True)
        self.api.ha_chassis_group_add_chassis(
            self.hcg_name, self.chassis, priority=10).execute(check_error=True)

        # Deletes the HA Chassis entry
        self.api.ha_chassis_group_del_chassis(
            self.hcg_name, self.chassis).execute(check_error=True)
        row = self.api.db_find(
            'HA_Chassis',
            ('chassis_name', '=', self.chassis)).execute(check_error=True)
        self.assertEqual([], row)

        # Tries to delete it again, since if_exists=True it shouldn't raise
        # any errors
        self.api.ha_chassis_group_del_chassis(
            self.hcg_name, self.chassis, if_exists=True).execute(
            check_error=True)

        # Tries to delete it again with if_exists=False, now it should raise
        # a RuntimeError
        cmd = self.api.ha_chassis_group_del_chassis(
            self.hcg_name, self.chassis, if_exists=False)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

        # Deletes the HA Chassis Group entry
        self.api.ha_chassis_group_del(self.hcg_name).execute(check_error=True)
        cmd = self.api.ha_chassis_group_get(self.hcg_name)
        self.assertRaises(idlutils.RowNotFound, cmd.execute, check_error=True)

        # Tries to delete it again, since if_exists=True it shouldn't raise
        # any errors
        self.api.ha_chassis_group_del(
            self.hcg_name, if_exists=True).execute(check_error=True)

        # Tries to delete it again with if_exists=False, now it should raise
        # a RuntimeError
        cmd = self.api.ha_chassis_group_del(self.hcg_name)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_ha_chassis_group_may_exist(self):
        cmd = self.api.ha_chassis_group_add(self.hcg_name, may_exist=True)
        hcg1 = cmd.execute(check_error=True)
        hcg2 = cmd.execute(check_error=True)
        self.assertEqual(hcg1, hcg2)


class TestReferencedObjects(OvnNorthboundTest):
    """Exercise adding a ls, lsp and lsp_address in a single transaction.

    The main goal of this test is to make sure a transaction can use either
    a name or an object notation in order to create an ls+lsp while in a
    transaction context.
    """
    def setUp(self):
        super(TestReferencedObjects, self).setUp()
        self.ls_name = utils.get_rand_device_name()
        self.lsp_name = utils.get_rand_device_name()
        self.lsp_test_addresses = ['de:ad:be:ef:4d:ad 192.0.2.1']

    def _check_values(self):
        # Check: Make sure ls_get and lsp_get work (no RowNotFound exception)
        self.api.ls_get(self.ls_name).execute(check_error=True)
        self.api.lsp_get(self.lsp_name).execute(check_error=True)
        self.assertEqual(self.lsp_test_addresses,
                         self.api.lsp_get_addresses(self.lsp_name).execute(
                             check_error=True))

    def test_lsp_add_by_name(self):
        with self.api.transaction(check_error=True) as txn:
            txn.add(self.api.ls_add(self.ls_name))
            txn.add(self.api.lsp_add(self.ls_name, self.lsp_name))
            txn.add(self.api.lsp_set_addresses(self.lsp_name,
                                               self.lsp_test_addresses))
        self._check_values()

    def test_lsp_add_by_object_via_db_create(self):
        with self.api.transaction(check_error=True) as txn:
            sw = txn.add(self.api.db_create_row('Logical_Switch',
                                                name=self.ls_name))
            prt = txn.add(self.api.db_create_row('Logical_Switch_Port',
                                                 name=self.lsp_name))
            txn.add(self.api.db_add('Logical_Switch', sw, "ports", prt))
            txn.add(self.api.db_add('Logical_Switch_Port', prt,
                                    "addresses", self.lsp_test_addresses[0]))
        self._check_values()


class TestMeterOps(OvnNorthboundTest):

    def setUp(self):
        super(TestMeterOps, self).setUp()
        self.table = self.api.tables['Meter']

    def _meter_add(self, name=None, unit=None, rate=1, fair=False,
                   burst_size=0, action=None, may_exist=False, **columns):
        if name is None:
            name = utils.get_rand_name()
        if unit is None:
            unit = 'pktps'
        # NOTE(flaviof): action drop is the only one option in the schema when
        #                this test was implemented. That is expected to be
        #                properly handled by api.
        exp_action = 'drop' if action is None else action
        meter = self.useFixture(fixtures.MeterFixture(
            self.api, name, unit, rate, fair, burst_size, action, may_exist,
            **columns)).obj
        meter_band = meter.bands[0]
        self.assertEqual(name, meter.name)
        self.assertEqual(unit, meter.unit)
        self.assertEqual([fair], meter.fair)
        self.assertEqual(exp_action, meter_band.action)
        self.assertEqual(rate, meter_band.rate)
        self.assertEqual(burst_size, meter_band.burst_size)
        return meter

    def _meter_del(self, row, col):
        name404 = utils.get_rand_name()
        self.api.meter_del(name404, if_exists=True).execute(check_error=True)
        self.assertIn(row.uuid, self.table.rows)
        self.api.meter_del(row.uuid).execute(check_error=True)
        self.assertNotIn(row.uuid, self.table.rows)
        self.api.meter_del(col, if_exists=True).execute(check_error=True)
        cmd = self.api.meter_del(col)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def _meter_get(self, col):
        meter = self._meter_add()
        val = getattr(meter, col)
        found = self.api.meter_get(val).execute(check_error=True)
        self.assertEqual(meter, found)

    def test_meter_add_non_defaults(self):
        self._meter_add(utils.get_rand_name(), "kbps", 321, True, 500, None)

    def test_meter_add_ext_ids(self):
        ext_ids = {
            utils.get_rand_name(prefix="random_"): utils.get_random_string(10)
            for _ in range(3)}
        meter = self._meter_add(external_ids=ext_ids)
        self.assertEqual(ext_ids, meter.external_ids)

    def test_meter_add_may_exist(self):
        cmd = self.api.meter_add("same_name", "kbps", may_exist=True)
        m1 = cmd.execute(check_error=True)
        m2 = cmd.execute(check_error=True)
        self.assertEqual(m1, m2)

    def test_meter_add_duplicate(self):
        cmd = self.api.meter_add("same_name", "kbps", may_exist=False)
        cmd.execute(check_error=True)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_meter_del_by_uuid(self):
        meter = self._meter_add()
        self._meter_del(meter, meter.uuid)

    def test_meter_del_by_name(self):
        name = utils.get_rand_name()
        meter = self._meter_add(name)
        self._meter_del(meter, name)

    def test_meter_list(self):
        meters = {self._meter_add() for _ in range(3)}
        meter_set = set(self.api.meter_list().execute(check_error=True))
        self.assertTrue(meters.issubset(meter_set))

    def test_meter_get_uuid(self):
        self._meter_get('uuid')

    def test_meter_get_name(self):
        self._meter_get('name')


class TestBFDOps(OvnNorthboundTest):

    def setUp(self):
        super(TestBFDOps, self).setUp()
        self.table = self.api.tables['BFD']

    @staticmethod
    def _freeze_and_filter_row(row, filter_columns):
        return types.SimpleNamespace(
            **{k: v
               for k, v in idlutils.frozen_row(row)._asdict().items()
               if k not in filter_columns})

    def _bfd_add(self, *args, **kwargs):
        cmd = self.api.bfd_add(*args, **kwargs)
        row = cmd.execute(check_error=True)
        self.assertEqual(cmd.logical_port, row.logical_port)
        self.assertEqual(cmd.dst_ip, row.dst_ip)
        self.assertEqual(cmd.columns['min_tx'] if cmd.columns[
            'min_tx'] else [], row.min_tx)
        self.assertEqual(cmd.columns['min_rx'] if cmd.columns[
            'min_rx'] else [], row.min_rx)
        self.assertEqual(cmd.columns['detect_mult'] if cmd.columns[
            'detect_mult'] else [], row.detect_mult)
        self.assertEqual(cmd.columns['external_ids'] or {}, row.external_ids)
        self.assertEqual(cmd.columns['options'] or {}, row.options)
        return self._freeze_and_filter_row(row, ('status',))

    def test_bfd_add(self):
        name = utils.get_rand_name()
        self._bfd_add(name, name)

    def test_bfd_add_non_defaults(self):
        name = utils.get_rand_name()
        self._bfd_add(
            name,
            name,
            min_rx=1,
            min_tx=2,
            detect_mult=3,
            external_ids={'a': 'A'},
            options={'b': 'B'},
            may_exist=True,
        )

    def test_bfd_add_duplicate(self):
        name = utils.get_rand_name()
        cmd = self.api.bfd_add(name, name)
        cmd.execute(check_error=True)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_bfd_add_may_exist_no_change(self):
        name = utils.get_rand_name()
        b1 = self._bfd_add(name, name)
        b2 = self._bfd_add(name, name, may_exist=True)
        self.assertEqual(b1, b2)

    def test_bfd_add_may_exist_change(self):
        name = utils.get_rand_name()
        b1 = self._bfd_add(name, name)
        b2 = self._bfd_add(
            name,
            name,
            min_rx=11,
            min_tx=22,
            detect_mult=33,
            external_ids={'aa': 'AA'},
            options={'bb': 'BB'},
            may_exist=True,
        )
        self.assertNotEqual(b1, b2)
        self.assertEqual(b1.uuid, b2.uuid)

    def test_bfd_del(self):
        name = utils.get_rand_name()
        b1 = self._bfd_add(name, name)
        self.assertIn(b1.uuid, self.table.rows)
        self.api.bfd_del(b1.uuid).execute(check_error=True)
        self.assertNotIn(b1.uuid, self.table.rows)

    def test_bfd_find(self):
        name1 = utils.get_rand_name()
        name2 = utils.get_rand_name()
        b1 = self._bfd_add(name1, name1)
        b2 = self._bfd_add(name1, name2)
        b3 = self._bfd_add(name2, name1)
        b4 = self._bfd_add(name2, name2)
        self.assertIn(b1.uuid, self.table.rows)
        self.assertIn(b2.uuid, self.table.rows)
        self.assertIn(b3.uuid, self.table.rows)
        self.assertIn(b4.uuid, self.table.rows)
        found = self.api.bfd_find(
            b1.logical_port,
            b1.dst_ip).execute(check_error=True)
        self.assertEqual(1, len(found))
        self.assertEqual(b1.uuid, found[0].uuid)
        for col in set(self.api.tables['BFD'].columns.keys()) - set(
                ('_uuid', 'status')):
            self.assertEqual(
                getattr(b1, col),
                getattr(found[0], col))

    def test_bfd_get(self):
        name = utils.get_rand_name()
        b1 = self._freeze_and_filter_row(
            self.api.bfd_add(name, name).execute(check_error=True),
            ('status',))
        b2 = self._freeze_and_filter_row(
            self.api.bfd_get(b1.uuid).execute(check_error=True),
            ('status',))
        self.assertEqual(b1, b2)


class TestMirrorOps(OvnNorthboundTest):

    def setUp(self):
        super(TestMirrorOps, self).setUp()
        self.table = self.api.tables['Mirror']
        self.switch = self.useFixture(
            fixtures.LogicalSwitchFixture(self.api)).obj
        lsp_add_cmd = self.api.lsp_add(self.switch.uuid, 'testport')
        with self.api.transaction(check_error=True) as txn:
            txn.add(lsp_add_cmd)

        self.port_uuid = lsp_add_cmd.result.uuid

    def _mirror_add(self, name=None, direction_filter='to-lport',
                    dest='10.11.1.1', mirror_type='gre', index=42, **kwargs):
        if not name:
            name = utils.get_rand_name()
        cmd = self.api.mirror_add(name, mirror_type, index, direction_filter,
                                  dest, **kwargs)
        row = cmd.execute(check_error=True)
        self.assertEqual(cmd.name, row.name)
        self.assertEqual(cmd.direction_filter, row.filter)
        self.assertEqual(cmd.dest, row.sink)
        self.assertEqual(cmd.mirror_type, row.type)
        self.assertEqual(cmd.index, row.index)
        return idlutils.frozen_row(row)

    def test_mirror_addx(self):
        self._mirror_add(dest='10.13.1.1')

    def test_mirror_add_duplicate(self):
        name = utils.get_rand_name()
        cmd = self.api.mirror_add(name, 'gre', 100, 'from-lport',
                                  '192.169.1.1')
        cmd.execute(check_error=True)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_mirror_add_may_exist_no_change(self):
        name = utils.get_rand_name()
        mirror1 = self._mirror_add(name=name, dest='10.18.1.1')
        mirror2 = self._mirror_add(name=name, dest='10.18.1.1',
                                   may_exist=True)
        self.assertEqual(mirror1, mirror2)

    def test_mirror_add_may_exist_change(self):
        name = utils.get_rand_name()
        mirror1 = self._mirror_add(name=name, dest='10.12.1.0')
        mirror2 = self._mirror_add(
            name=name, direction_filter='from-lport', dest='10.12.1.0',
            mirror_type='gre', index=100, may_exist=True,
        )
        self.assertNotEqual(mirror1, mirror2)
        self.assertEqual(mirror1.uuid, mirror2.uuid)

    def test_mirror_del(self):
        name = utils.get_rand_name()
        mirror1 = self._mirror_add(name=name, dest='10.14.1.0')
        self.assertIn(mirror1.uuid, self.table.rows)
        self.api.mirror_del(mirror1.uuid).execute(check_error=True)
        self.assertNotIn(mirror1.uuid, self.table.rows)

    def test_mirror_get(self):
        name = utils.get_rand_name()
        mirror1 = self.api.mirror_add(name, 'gre', 100, 'from-lport',
                                      '10.15.1.1').execute(check_error=True)
        mirror2 = self.api.mirror_get(mirror1.uuid).execute(check_error=True)
        self.assertEqual(mirror1, mirror2)

    def test_lsp_attach_detach_mirror(self):
        mirror = self._mirror_add(name='my_mirror')
        self.api.lsp_attach_mirror(
            self.port_uuid, mirror.uuid).execute(check_error=True)
        port = self.api.lsp_get(self.port_uuid).execute(check_error=True)

        self.assertEqual(1, len(port.mirror_rules))
        mir_rule = self.api.lookup('Mirror', port.mirror_rules[0].uuid)
        self.assertEqual(mirror.uuid, mir_rule.uuid)

        self.api.lsp_detach_mirror(
            self.port_uuid, mirror.uuid).execute(check_error=True)
        port = self.api.lsp_get(self.port_uuid).execute(check_error=True)

        self.assertEqual(0, len(port.mirror_rules))

    def test_lsp_attach_detach_may_exist(self):
        mirror1 = self._mirror_add(name='mirror1')
        self.api.lsp_attach_mirror(
            self.port_uuid, mirror1.uuid).execute(check_error=True)
        mirror2 = self._mirror_add(name='mirror2', dest='10.17.1.0')

        # Try to attach a mirror to a port which already has mirror_rule
        # attached
        failing_cmd = self.api.lsp_attach_mirror(
            self.port_uuid, mirror1.uuid,
            may_exist=False)
        self.assertRaises(
            RuntimeError,
            failing_cmd.execute,
            check_error=True
        )

        self.api.lsp_attach_mirror(
            self.port_uuid, mirror2.uuid,
            may_exist=True).execute(check_error=True)
        check_res = self.api.lsp_get(self.port_uuid).execute(check_error=True)
        rule_on_lsp = False
        for m_rule in check_res.mirror_rules:
            if mirror2.uuid == m_rule.uuid:
                rule_on_lsp = True
        self.assertTrue(rule_on_lsp)

        self.api.lsp_detach_mirror(
            self.port_uuid, mirror2.uuid).execute(check_error=True)
        port = self.api.lsp_get(self.port_uuid).execute(check_error=True)
        self.assertEqual(1, len(port.mirror_rules))
        self.assertEqual(mirror1.uuid, port.mirror_rules[0].uuid)

        # Try to detach a rule that is already detached
        failing_cmd = self.api.lsp_detach_mirror(
            self.port_uuid, mirror2.uuid)
        self.assertRaises(
            RuntimeError,
            failing_cmd.execute,
            check_error=True
        )

        # detach with if_exist=True, and check the result, to be as previously
        self.api.lsp_detach_mirror(
            self.port_uuid, mirror2.uuid,
            if_exist=True).execute(check_error=True)
        self.assertEqual(1, len(port.mirror_rules))
        self.assertEqual(mirror1.uuid, port.mirror_rules[0].uuid)
