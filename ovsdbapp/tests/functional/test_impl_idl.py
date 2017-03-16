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

from ovsdbapp import constants
from ovsdbapp import impl_idl
from ovsdbapp.tests import base
from ovsdbapp.tests import utils


class DefaultContext(object):
    ovsdb_connection = constants.DEFAULT_OVSDB_CONNECTION
    vsctl_timeout = constants.DEFAULT_TIMEOUT


class TestOvsdbIdl(base.TestCase):

    def setUp(self):
        super(TestOvsdbIdl, self).setUp()
        self.api = impl_idl.OvsdbIdl(DefaultContext())
        self.brname = utils.get_rand_device_name()
        # Destroying the bridge cleans up most things created by tests
        cleanup_cmd = self.api.del_br(self.brname)
        self.addCleanup(cleanup_cmd.execute)

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

    def _test_add_port(self):
        pname = utils.get_rand_device_name()
        with self.api.transaction(check_error=True) as txn:
            txn.add(self.api.add_br(self.brname))
            txn.add(self.api.add_port(self.brname, pname))
        return pname

    def test_add_port(self):
        pname = self._test_add_port()
        plist_cmd = self.api.list_ports(self.brname)
        ports = plist_cmd.execute(check_error=True)
        self.assertIn(pname, ports)

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
