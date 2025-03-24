# Copyright (c) 2015 Red Hat, Inc.
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

import logging
import os
import queue
import threading
import time
import traceback

from ovs.db import custom_index
from ovs.db import idl
from ovs import poller

from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import exceptions

if os.name == 'nt':
    from ovsdbapp.backend.ovs_idl.windows import connection_utils
else:
    from ovsdbapp.backend.ovs_idl.linux import connection_utils

LOG = logging.getLogger(__name__)


class TransactionQueue(queue.Queue, object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._wait_queue = connection_utils.WaitQueue(
            max_queue_size=self.maxsize)

    def get_nowait(self, *args, **kwargs):
        try:
            result = super().get_nowait(*args, **kwargs)
        except queue.Empty:
            return None
        self._wait_queue.alert_notification_consume()
        return result

    def put(self, *args, **kwargs):
        super().put(*args, **kwargs)
        self.alert_notify()

    @property
    def alert_fileno(self):
        return self._wait_queue.alert_fileno

    def alert_notify(self):
        self._wait_queue.alert_notify()


class Connection(object):

    def __init__(self, idl, timeout):
        """Create a connection to an OVSDB server using the OVS IDL

        :param timeout: The timeout value for OVSDB operations
        :param idl: A newly created ovs.db.Idl instance (run never called)
        """
        self.timeout = timeout
        self.txns = TransactionQueue(1)
        self.lock = threading.RLock()
        self.idl = idl
        self.thread = None
        self.is_running = None

    def start(self):
        """Start the connection."""
        with self.lock:
            if self.thread is not None:
                return False
            if not self.idl.has_ever_connected() or self.is_running is False:
                if self.is_running is False:  # stop() was called
                    self.idl.force_reconnect()
                idlutils.wait_for_change(self.idl, self.timeout)
                try:
                    self.idl.post_connect()
                except AttributeError:
                    # An ovs.db.Idl class has no post_connect
                    pass
            self.poller = poller.Poller()
            self.is_running = True
            self.thread = threading.Thread(target=self.run)
            self.thread.daemon = True
            self.thread.start()

    def run(self):
        while self.is_running:
            # If we fail in an Idl call, we could have missed an update
            # from the server, leaving us out of sync with ovsdb-server.
            # It is not safe to continue without restarting the connection.
            # Though it is likely that the error is unrecoverable, keep trying
            # indefinitely just in case.
            try:
                self.idl.wait(self.poller)
                self.poller.fd_wait(self.txns.alert_fileno, poller.POLLIN)
                self.poller.block()
                with self.lock:
                    self.idl.run()
            except Exception as e:
                # This shouldn't happen, but is possible if there is a bug
                # in python-ovs, or an unhandled exception in overridden
                # Idl.notify() code
                LOG.exception(e)
                continue
            txn = self.txns.get_nowait()
            if txn is not None:
                try:
                    with self.lock:
                        txn.results.put(txn.do_commit())
                except Exception as ex:
                    er = idlutils.ExceptionResult(ex=ex,
                                                  tb=traceback.format_exc())
                    txn.results.put(er)
                self.txns.task_done()
        self.idl.close()

    def stop(self, timeout=None):
        if not self.is_running:
            return True
        self.is_running = False
        self.txns.put(None)
        self.thread.join(timeout)
        if self.thread.is_alive():
            return False
        self.thread = None
        return True

    def queue_txn(self, txn):
        # Even if we aren't started, we can queue a transaction and it will
        # run when we are started
        try:
            self.txns.put(txn, timeout=self.timeout)
        except queue.Full as e:
            raise exceptions.TimeoutException(commands=txn.commands,
                                              timeout=self.timeout,
                                              cause='TXN queue is full') from e

    def force_reconnect(self):
        self.idl.force_reconnect()
        self.txns.alert_notify()


class OvsdbIdl(idl.Idl):
    def cooperative_yield(self):
        time.sleep(0)

    @classmethod
    def from_server(cls, connection_string, schema_name, *args,
                    helper=None, helper_tables=None, **kwargs):
        """Create the Idl instance by pulling the schema from OVSDB server

        :param connection_string: Connection name
        :type connection_string: string
        :param schema_name: Schema name
        :type schema: string
        :param helper: Helper instance
        :type helper: ``idl.SchemaHelper``
        :param helper_tables: Tables to be registered in the helper
        :type helper_tables: Iterator of strings
        """
        if not helper:
            helper = idlutils.get_schema_helper(connection_string, schema_name)

        if not helper_tables:
            helper.register_all()
        else:
            for table in helper_tables:
                helper.register_table(table)
        return cls(connection_string, helper, **kwargs)

    def post_connect(self):
        """Operations to execute after the Idl has connected to the server

        An example would be to set up Idl notification handling for watching
        and unwatching certain OVSDB change events
        """

    def update_tables(self, tables, schema):
        """Add the tables to the current Idl if they are present in the schema

        :param tables: List of tables to be registered
        :type tables: List
        :param schema: Schema description
        :type schema: dict or string
        """

        schema_helper = idlutils.create_schema_helper(schema)

        # Register only available registered tables - DB downgrade, and the
        # newly added tables - DB upgrade
        for table in self.tables.keys() | tables:
            if table in schema_helper.schema_json['tables']:
                schema_helper.register_table(table)

        schema = schema_helper.get_idl_schema()
        self._db = schema
        removed = self.tables.keys() - schema.tables.keys()
        added = schema.tables.keys() - self.tables.keys()

        # stop monitoring removed tables
        for table in removed:
            self.cond_change(table, [False])
            del self.tables[table]

        try:
            _tables = idl.IdlTable.schema_tables(self, schema)
        except AttributeError:
            _tables = None
        # add new tables as Idl.__init__ does
        for table in (schema.tables[table] for table in added):
            self.tables[table.name] = _tables[table.name] if _tables else table
            for column in table.columns.values():
                if not hasattr(column, 'alert'):
                    column.alert = True
            table.need_table = False
            table.rows = custom_index.IndexedRows(table)
            table.idl = self
            try:
                table.condition = idl.ConditionState()
            except AttributeError:
                table.condition = [True]
            table.cond_changed = False
