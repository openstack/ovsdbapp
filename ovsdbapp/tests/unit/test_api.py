# Copyright (c) 2017 Red Hat, Inc.
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

import mock
import testtools

from ovsdbapp import api
from ovsdbapp.tests import base


class FakeTransaction(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, tb):
        self.commit()

    def commit(self):
        """Serves just for mock."""


class TestingAPI(api.API):
    def create_transaction(self, check_error=False, log_errors=True, **kwargs):
        return FakeTransaction()


TestingAPI.__abstractmethods__ = set()


class TransactionTestCase(base.TestCase):
    def setUp(self):
        super(TransactionTestCase, self).setUp()
        self.api = TestingAPI()
        mock.patch.object(FakeTransaction, 'commit').start()

    def test_transaction_nested(self):
        with self.api.transaction() as txn1:
            with self.api.transaction() as txn2:
                self.assertIs(txn1, txn2)
        txn1.commit.assert_called_once_with()

    def test_transaction_no_nested_transaction_after_error(self):
        class TestException(Exception):
            pass

        with testtools.ExpectedException(TestException):
            with self.api.transaction() as txn1:
                raise TestException()

        with self.api.transaction() as txn2:
            self.assertIsNot(txn1, txn2)
