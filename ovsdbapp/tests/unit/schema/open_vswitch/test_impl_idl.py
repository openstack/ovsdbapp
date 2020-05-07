# Copyright (c) 2016 Red Hat, Inc.
#
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

from unittest import mock

import testtools

from ovsdbapp import exceptions
from ovsdbapp.schema.open_vswitch import impl_idl
from ovsdbapp.tests import base


class TransactionTestCase(base.TestCase):
    def test_commit_raises_exception_on_timeout(self):
        transaction = impl_idl.OvsVsctlTransaction(mock.sentinel,
                                                   mock.Mock(), 1)
        with testtools.ExpectedException(exceptions.TimeoutException):
            transaction.commit()

    def test_post_commit_does_not_raise_exception(self):
        with mock.patch.object(impl_idl.OvsVsctlTransaction,
                               "do_post_commit", side_effect=Exception):
            transaction = impl_idl.OvsVsctlTransaction(mock.sentinel,
                                                       mock.Mock(), 0)
            transaction.post_commit(mock.Mock())


class TestOvsdbIdl(base.TestCase):
    def setUp(self):
        super(TestOvsdbIdl, self).setUp()
        impl_idl.OvsdbIdl.ovsdb_connection = None

    def test_nested_txns(self):
        conn = mock.MagicMock()
        api = impl_idl.OvsdbIdl(conn, nested_transactions=False)
        self.assertFalse(api._nested_txns)

    def test_init_session(self):
        conn = mock.MagicMock()
        impl_idl.OvsdbIdl(conn, start=False)
        conn.start_connection.assert_not_called()
