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

from unittest import mock

from ovsdbapp import exceptions as exc
from ovsdbapp.schema.open_vswitch import impl_idl
from ovsdbapp.tests.functional import base
from ovsdbapp.tests import utils


# NOTE(twilson) functools.partial does not work for this
def trpatch(*args, **kwargs):
    def wrapped(fn):
        return mock.patch.object(impl_idl.OvsVsctlTransaction,
                                 *args, **kwargs)(fn)
    return wrapped


class TestOvsdbIdl(base.FunctionalTestCase):
    schemas = ["Open_vSwitch"]

    def setUp(self):
        super(TestOvsdbIdl, self).setUp()
        self.api = impl_idl.OvsdbIdl(self.connection)
        self.brname = utils.get_rand_device_name()
        # Destroying the bridge cleans up most things created by tests
        cleanup_cmd = self.api.del_br(self.brname)
        self.addCleanup(cleanup_cmd.execute)

    def test_idl_run_exception_terminates(self):
        run = self.api.idl.run
        with mock.patch.object(self.api.idl, "run") as runmock:
            exceptions = iter([Exception("TestException")])

            def side_effect():
                try:
                    raise next(exceptions)
                except StopIteration:
                    return run()

            runmock.side_effect = side_effect
            exists = self.api.br_exists(self.brname).execute(check_error=True)
            self.assertFalse(exists)

    def test_br_exists_false(self):
        exists = self.api.br_exists(self.brname).execute(check_error=True)
        self.assertFalse(exists)

    def test_add_br_may_exist(self):
        self.api.add_br(self.brname).execute(check_error=True)
        with self.api.transaction(check_error=True) as txn:
            txn.add(self.api.add_br(self.brname, datapath_type="netdev"))
            exists = txn.add(self.api.br_exists(self.brname))
            dpt = txn.add(self.api.db_get("Bridge", self.brname,
                                          "datapath_type"))
        self.assertTrue(exists)
        self.assertEqual("netdev", dpt.result)

    def test_add_br_may_not_exist(self):
        self.api.add_br(self.brname).execute(check_error=True)
        cmd = self.api.add_br(self.brname, may_exist=False)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_del_br_if_exists_false(self):
        cmd = self.api.del_br(self.brname, if_exists=False)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_del_br_if_exists_true(self):
        self.api.del_br(self.brname).execute(check_error=True)

    def test_del_br(self):
        self.api.add_br(self.brname).execute(check_error=True)
        self.api.del_br(self.brname).execute(check_error=True)
        exists = self.api.br_exists(self.brname).execute(check_error=True)
        self.assertFalse(exists)

    def _test_add_port(self, **interface_attrs):
        pname = utils.get_rand_device_name()
        with self.api.transaction(check_error=True) as txn:
            txn.extend([self.api.add_br(self.brname),
                        self.api.add_port(self.brname, pname,
                                          **interface_attrs)])
        return pname

    def test_add_port(self):
        interface_attrs = {'external_ids': {'iface-id': 'port_iface-id'},
                           'type': 'internal'}
        pname = self._test_add_port(**interface_attrs)
        plist_cmd = self.api.list_ports(self.brname)
        ports = plist_cmd.execute(check_error=True)
        self.assertIn(pname, ports)
        with self.api.transaction(check_error=True) as txn:
            external_ids = txn.add(self.api.db_get('Interface', pname,
                                                   'external_ids'))
            _type = txn.add(self.api.db_get('Interface', pname, 'type'))
        self.assertEqual(interface_attrs['external_ids'], external_ids.result)
        self.assertEqual(interface_attrs['type'], _type.result)

    def test_add_port_wrong_interface_attrs(self):
        interface_attrs = {'invalid_interface_field': 'value'}
        self.assertRaises(KeyError, self._test_add_port, **interface_attrs)

    def test_add_port_may_exist_false(self):
        pname = self._test_add_port()
        cmd = self.api.add_port(self.brname, pname, may_exist=False)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_del_port(self):
        pname = self._test_add_port()
        plist_cmd = self.api.list_ports(self.brname)
        self.assertIn(pname, plist_cmd.execute(check_error=True))
        self.api.del_port(pname).execute(check_error=True)
        self.assertNotIn(pname, plist_cmd.execute(check_error=True))

    def test_del_port_if_exists_false(self):
        cmd = self.api.del_port(utils.get_rand_device_name(), if_exists=False)
        self.assertRaises(RuntimeError, cmd.execute, check_error=True)

    def test_connection_reconnect(self):
        self.api.ovsdb_connection.stop()
        existsCmd = self.api.br_exists(self.brname)
        txn = self.api.create_transaction(check_error=True)
        txn.add(existsCmd)
        self.api.ovsdb_connection.queue_txn(txn)
        self.api.ovsdb_connection.start()
        result = txn.results.get(timeout=self.api.ovsdb_connection.timeout)
        self.assertEqual(result, [False])

    def test_connection_disconnect_timeout(self):
        is_running_mock = mock.PropertyMock(return_value=True)
        connection = self.api.ovsdb_connection
        type(connection).is_running = is_running_mock
        self.addCleanup(delattr, type(connection), 'is_running')
        self.assertFalse(connection.stop(1))

    def test_br_external_id(self):
        KEY = "foo"
        VALUE = "bar"
        self.api.add_br(self.brname).execute(check_error=True)
        self.api.br_set_external_id(self.brname, KEY, VALUE).execute(
            check_error=True)
        external_id = self.api.br_get_external_id(self.brname, KEY).execute(
            check_error=True)
        self.assertEqual(VALUE, external_id)

    def test_iface_external_id(self):
        KEY = "foo"
        VALUE = "bar"
        self.api.add_br(self.brname).execute(check_error=True)
        self.api.iface_set_external_id(self.brname, KEY, VALUE).execute(
            check_error=True)
        external_id = self.api.iface_get_external_id(self.brname, KEY).execute(
            check_error=True)
        self.assertEqual(VALUE, external_id)


class ImplIdlTestCase(base.FunctionalTestCase):
    schemas = ['Open_vSwitch']

    def setUp(self):
        super(ImplIdlTestCase, self).setUp()
        self.api = impl_idl.OvsdbIdl(self.connection)
        self.brname = utils.get_rand_device_name()
        # Make sure exceptions pass through by calling do_post_commit directly
        mock.patch.object(
            impl_idl.OvsVsctlTransaction, "post_commit",
            side_effect=impl_idl.OvsVsctlTransaction.do_post_commit,
            autospec=True).start()

    def _add_br(self):
        # NOTE(twilson) we will be raising exceptions with add_br, so schedule
        # cleanup before that.
        cmd = self.api.del_br(self.brname)
        self.addCleanup(cmd.execute)
        with self.api.transaction(check_error=True) as tr:
            tr.add(self.api.add_br(self.brname))
        return tr

    def _add_br_and_test(self):
        self._add_br()
        ofport = self.api.db_get("Interface", self.brname, "ofport").execute(
            check_error=True)
        self.assertTrue(int(ofport))
        self.assertGreater(ofport, -1)

    def test_post_commit_vswitchd_completed_no_failures(self):
        self._add_br_and_test()

    @trpatch("vswitchd_has_completed", return_value=True)
    @trpatch("post_commit_failed_interfaces", return_value=["failed_if1"])
    @trpatch("timeout_exceeded", return_value=False)
    def test_post_commit_vswitchd_completed_failures(self, *args):
        self.assertRaises(impl_idl.VswitchdInterfaceAddException,
                          self._add_br)

    @trpatch("vswitchd_has_completed", return_value=False)
    def test_post_commit_vswitchd_incomplete_timeout(self, *args):
        # Due to timing issues we may rarely hit the global timeout, which
        # raises RuntimeError to match the vsctl implementation
        mock.patch('ovsdbapp.backend.ovs_idl.transaction.'
                   'Transaction.timeout_exceeded', return_value=True).start()
        self.assertRaises((exc.TimeoutException, RuntimeError), self._add_br)
