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

import queue
import time
from unittest import mock
import uuid

from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp.backend.ovs_idl import event
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import constants
from ovsdbapp.schema.ovn_northbound import impl_idl
from ovsdbapp.tests.functional import base
from ovsdbapp.tests.functional.schema.ovn_northbound import test_impl_idl


def create_schema_helper(sh):
    return sh


class TestingOvsIdl(connection.OvsdbIdl):
    schema = 'Open_vSwitch'

    @classmethod
    def from_server(cls, schema_map, tables):
        """Create the Idl instance by pulling the schema from OVSDB server"""
        connection_string = schema_map[cls.schema]
        helper = idlutils.get_schema_helper(connection_string, cls.schema)
        for table in tables:
            helper.register_table(table)
        return cls(connection_string, helper)


class TestOvsdbIdl(base.FunctionalTestCase):
    default_tables = ["Open_vSwitch", "Bridge"]

    def setUp(self):
        super().setUp()
        self.schema = self.get_schema()

    @property
    def idl(self):
        return self._connection.idl

    @classmethod
    def set_connection(cls):
        idl = TestingOvsIdl.from_server(cls.schema_map, cls.default_tables)
        cls._connection = connection.Connection(idl, constants.DEFAULT_TIMEOUT)

    def get_schema(self):
        with mock.patch.object(
                idlutils, 'create_schema_helper',
                side_effect=create_schema_helper):
            return idlutils.get_schema_helper(
                self.schema_map[TestingOvsIdl.schema], TestingOvsIdl.schema)

    def validate_tables(self, tables, present):
        valid_func = self.assertIn if present else self.assertNotIn

        for table in tables:
            valid_func(table, self.idl.tables)

        # ensure that the Idl still works after we update the tables
        self.idl.run()

    def test_add_new_table(self):
        tables = ["Port", "Interface"]

        self.validate_tables(tables, present=False)
        self.idl.update_tables(tables, self.schema)
        self.validate_tables(tables, present=True)

    def test_remove_table(self):
        removed_table = "Open_vSwitch"
        new_tables = self.default_tables[:]
        new_tables.remove(removed_table)

        self.validate_tables([removed_table], present=True)

        del self.schema["tables"][removed_table]
        self.idl.update_tables(self.default_tables, self.schema)

        self.validate_tables(new_tables, present=True)
        self.validate_tables([removed_table], present=False)


class LsCreateWaitEvent(event.WaitEvent):
    ONETIME = True

    def __init__(self, lsp_name, timeout):
        super().__init__((self.ROW_CREATE,), "Logical_Switch",
                         (("name", "=", lsp_name),), timeout=timeout)


class TestConnectionReconnect(test_impl_idl.OvnNorthboundTest):

    def setUp(self):
        super().setUp()
        # seed the db with an LS prior to setting up notifications
        self.ls = self._ls_add("test")
        self.handler = event.RowEventHandler()
        self.api.idl.notify = self.handler.notify

    def _ls_add(self, name):
        self.api.ls_add(name).execute(check_error=True)
        ls = self.api.ls_get(name).execute(check_error=True)
        self.assertEqual(name, ls.name)
        return ls

    def _create_and_watch_wait_event(self, name):
        event = LsCreateWaitEvent(name, timeout=10)
        self.handler.watch_event(event)
        return event

    def test_force_reconnect(self):
        event = self._create_and_watch_wait_event(self.ls.name)
        self.api.ovsdb_connection.force_reconnect()
        self.assertTrue(event.wait())
        # test things work after reconnect
        event = self._create_and_watch_wait_event("test2")
        self._ls_add("test2")
        self.assertTrue(event.wait())


class LockRecordingHandler(event.RowEventHandler):
    """A RowEventHandler that records lock transitions for inspection.

    lock_acquired/lock_lost run on the notify_loop thread; they push the
    lock name onto a queue so a test can block on the transition instead of
    polling.
    """

    def __init__(self):
        super().__init__()
        self.acquired = queue.Queue()
        self.lost = queue.Queue()

    def lock_acquired(self, lock_name):
        self.acquired.put(lock_name)

    def lock_lost(self, lock_name):
        self.lost.put(lock_name)


class PerInstanceNbApi(impl_idl.OvnNbApiIdlImpl):
    """OvnNbApiIdlImpl that does not share ovsdb_connection across instances.

    The default Backend stores ovsdb_connection in a class attribute, so
    every instance (and every test) would reuse a single connection and a
    single lock. Store it per instance instead, so each worker gets its own
    connection and lock.
    """

    @property
    def ovsdb_connection(self):
        return self.__dict__.get('_conn')

    @ovsdb_connection.setter
    def ovsdb_connection(self, conn):
        self.__dict__['_conn'] = conn


class TestLockNotify(test_impl_idl.OvnNorthboundTest):
    """Test the lock_acquired/lock_lost callbacks.

    A one-shot ``if idl.has_lock`` check races with lock acquisition: a worker
    granted the OVSDB lock after the check runs never reacts to it. The
    lock_acquired/lock_lost hooks let a worker respond to lock transitions
    whenever they happen. These tests wire the hooks up and assert they fire
    on real transitions.
    """

    LOCK_TIMEOUT = 10

    def _add_worker(self, lock_name, handler):
        connstr = self.schema_map['OVN_Northbound']
        idl = connection.OvsdbIdl.from_server(connstr, 'OVN_Northbound')
        idl.set_lock(lock_name)
        idl.notify_lock = handler.notify_lock
        conn = connection.Connection(idl, constants.DEFAULT_TIMEOUT)
        worker = PerInstanceNbApi(conn, auto_index=False)
        self.addCleanup(worker.ovsdb_connection.stop)
        return worker

    def _make_handler(self):
        handler = LockRecordingHandler()
        self.addCleanup(handler.shutdown)
        return handler

    def _wait_until(self, predicate):
        stop = time.time() + self.LOCK_TIMEOUT
        while time.time() < stop:
            if predicate():
                return True
            time.sleep(0.05)
        return False

    def test_lock_acquired(self):
        # A worker with lock callbacks wired acquires an uncontended lock;
        # the lock_acquired hook fires with the lock name and has_lock is set.
        lock_name = 'lock_%s' % uuid.uuid4().hex
        handler = self._make_handler()
        worker = self._add_worker(lock_name, handler)

        self.assertEqual(lock_name,
                         handler.acquired.get(timeout=self.LOCK_TIMEOUT))
        self.assertTrue(self._wait_until(lambda: worker.idl.has_lock))
        self.assertTrue(handler.lost.empty())

    def test_lock_lost_and_reacquired_on_reconnect(self):
        # Acquire the lock, then force a reconnect. The connection drops the
        # lock (lock_lost) and, being the only requester, reacquires it
        # (lock_acquired) -- both hooks fire without any second worker.
        lock_name = 'lock_%s' % uuid.uuid4().hex
        handler = self._make_handler()
        worker = self._add_worker(lock_name, handler)

        self.assertEqual(lock_name,
                         handler.acquired.get(timeout=self.LOCK_TIMEOUT))
        self.assertTrue(self._wait_until(lambda: worker.idl.has_lock))

        worker.ovsdb_connection.force_reconnect()

        self.assertEqual(lock_name,
                         handler.lost.get(timeout=self.LOCK_TIMEOUT))
        self.assertEqual(lock_name,
                         handler.acquired.get(timeout=self.LOCK_TIMEOUT))
        self.assertTrue(self._wait_until(lambda: worker.idl.has_lock))
