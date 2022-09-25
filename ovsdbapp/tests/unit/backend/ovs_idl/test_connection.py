# Copyright 2015, Red Hat, Inc.
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

from ovs import poller

from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.tests import base


@mock.patch.object(connection.threading, 'Thread')
class TestOVSNativeConnection(base.TestCase):

    @mock.patch.object(connection, 'TransactionQueue')
    def setUp(self, mock_trans_queue):
        super(TestOVSNativeConnection, self).setUp()
        self.idl = mock.Mock()
        self.mock_trans_queue = mock_trans_queue
        self.conn = connection.Connection(self.idl, timeout=1)
        self.mock_trans_queue.assert_called_once_with(1)

    @mock.patch.object(poller, 'Poller')
    @mock.patch.object(idlutils, 'wait_for_change')
    def test_start(self, mock_wait_for_change, mock_poller, mock_thread):
        self.idl.has_ever_connected.return_value = False
        self.conn.start()
        self.idl.has_ever_connected.assert_called_once()
        mock_wait_for_change.assert_any_call(self.conn.idl, self.conn.timeout)
        mock_poller.assert_called_once_with()
        mock_thread.assert_called_once_with(target=self.conn.run)
        self.assertIs(True, mock_thread.return_value.daemon)
        mock_thread.return_value.start.assert_called_once_with()

    def test_queue_txn(self, mock_thread):
        self.conn.start()
        self.conn.queue_txn('blah')
        self.conn.txns.put.assert_called_once_with('blah',
                                                   timeout=self.conn.timeout)


class TestTransactionQueue(base.TestCase):

    def test_init(self):
        # a test to cover py34 failure during initialization (LP Bug #1580270)
        # make sure no ValueError: can't have unbuffered text I/O is raised
        connection.TransactionQueue()
