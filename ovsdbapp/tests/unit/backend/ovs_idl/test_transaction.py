# Copyright (c) 2026 Red Hat LLC
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

import testscenarios

from ovsdbapp.backend.ovs_idl import transaction
from ovsdbapp.tests import base


load_tests = testscenarios.load_tests_apply_scenarios


class TestTransaction(base.TestCase):
    scenarios = [
        ('check_error', {'check_error': True}),
        ('ignore_error', {'check_error': False}),
    ]

    @mock.patch.object(transaction.Transaction, 'pre_commit')
    def test_pre_commit_exception_aborts(self, pre_commit):
        # Use a real python-ovs transaction to check that abort clears idl.txn.
        api = mock.Mock(idl=mock.Mock(txn=None, change_seqno=0))
        txn = transaction.Transaction(api, mock.Mock(timeout=1),
                                      check_error=self.check_error)
        command = txn.add(mock.Mock())
        error = RuntimeError('pre_commit failed')
        pre_commit.side_effect = error

        raised = self.assertRaises(RuntimeError, txn.do_commit)

        self.assertIs(error, raised)
        self.assertIsNone(api.idl.txn)
        command.run_idl.assert_not_called()
