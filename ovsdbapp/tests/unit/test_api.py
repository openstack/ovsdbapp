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

import sys
from unittest import mock

import fixtures
import testtools

from ovsdbapp import api
from ovsdbapp.tests import base

try:
    import eventlet
    from eventlet.green import thread

    sleep = eventlet.sleep

    def create_thread(executable):
        eventlet.spawn_n(executable)

except ImportError:
    import threading
    import time

    sleep = time.sleep

    def create_thread(executable):
        thread = threading.Thread(target=executable)
        thread.start()


class GreenThreadingFixture(fixtures.Fixture):
    def _setUp(self):
        if 'eventlet' in sys.modules:
            self._orig = api.threading.get_ident
            api.threading.get_ident = thread.get_ident
            self.addCleanup(self.cleanup)

    def cleanup(self):
        api.threading.get_ident = self._orig


class FakeTransaction(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, tb):
        self.commit()

    def commit(self):
        """Serves just for mock."""


class TestingAPI(api.API):
    def create_transaction(self, check_error=False, log_errors=True, **kwargs):
        txn = FakeTransaction()
        mock.patch.object(txn, 'commit').start()
        return txn


TestingAPI.__abstractmethods__ = set()


class TransactionTestCase(base.TestCase):
    def setUp(self):
        super(TransactionTestCase, self).setUp()
        self.api = TestingAPI()
        self.useFixture(GreenThreadingFixture())

    def test_transaction_nested(self):
        with self.api.transaction() as txn1:
            with self.api.transaction() as txn2:
                self.assertIs(txn1, txn2)
        txn1.commit.assert_called_once_with()

    def test_transaction_nested_false(self):
        with self.api.transaction(nested=False) as txn1:
            with self.api.transaction() as txn2:
                self.assertIsNot(txn1, txn2)
        txn1.commit.assert_called_once_with()
        txn2.commit.assert_called_once_with()

    def test_api_level_transaction_nested_fales(self):
        api = TestingAPI(nested_transactions=False)
        with api.transaction() as txn1:
            with api.transaction() as txn2:
                self.assertIsNot(txn1, txn2)
        txn1.commit.assert_called_once_with()
        txn2.commit.assert_called_once_with()

    def test_transaction_no_nested_transaction_after_error(self):
        class TestException(Exception):
            pass

        with testtools.ExpectedException(TestException):
            with self.api.transaction() as txn1:
                raise TestException()

        with self.api.transaction() as txn2:
            self.assertIsNot(txn1, txn2)

    def test_transaction_nested_multiple_threads(self):
        shared_resource = []

        def thread1():
            with self.api.transaction() as txn:
                shared_resource.append(txn)
                while len(shared_resource) == 1:
                    sleep(0.1)
            shared_resource.append(0)

        def thread2():
            while len(shared_resource) != 1:
                sleep(0.1)
            with self.api.transaction() as txn:
                shared_resource.append(txn)
            shared_resource.append(0)

        create_thread(thread1)
        create_thread(thread2)

        while len(shared_resource) != 4:
            sleep(0.1)

        txn1, txn2 = shared_resource[:2]

        self.assertNotEqual(txn1, txn2)
