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
import uuid

from ovsdbapp.backend.ovs_idl import command as cmd
from ovsdbapp.backend.ovs_idl import event
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.backend.ovs_idl import transaction
from ovsdbapp import exceptions

LOG = logging.getLogger(__name__)
_NO_DEFAULT = object()


class LookupWaitEvent(event.WaitEvent):

    def __init__(self, backend, table, record, timeout):
        events = (self.ROW_CREATE, self.ROW_UPDATE)
        super().__init__(events, table, None, timeout=timeout)
        self.backend = backend
        self.record = record
        self.event_name = 'LookupWaitEvent_%s' % table
        self.result = None

    def match_fn(self, event, row, old):
        try:
            # Normally, we would use run() to do things on match, but in this
            # case, that would mean we'd have to run lookup() again.
            with self.backend.ovsdb_connection.lock:
                self.result = self.backend.lookup(self.table, self.record)
            return bool(self.result)
        except idlutils.RowNotFound:
            return False


class Backend(object):
    lookup_table = {}
    _ovsdb_connection = None

    def __init__(self, connection, start=True, auto_index=True, **kwargs):
        super().__init__(**kwargs)
        self.ovsdb_connection = connection
        if auto_index:
            if connection.is_running:
                LOG.debug("Connection already started, not creating indices")
            else:
                self.autocreate_indices()
        if start:
            self.start_connection(connection)

    @property
    def ovsdb_connection(self):
        return self.__class__._ovsdb_connection

    @ovsdb_connection.setter
    def ovsdb_connection(self, connection):
        if self.__class__._ovsdb_connection is None:
            self.__class__._ovsdb_connection = connection

    def create_index(self, table, *columns):
        """Create a multi-column index on a table

        :param table:   The table on which to create an index
        :type table:    string
        :param columns: The columns in the index
        :type columns:  string
        """

        index_name = idlutils.index_name(*columns)
        idx = self.tables[table].rows.index_create(index_name)
        idx.add_columns(*columns)

    def autocreate_indices(self):
        """Create simple one-column indexes

        This creates indexes for all lookup_table entries and for all defined
        indexes columns in the OVSDB schema, as long as they are simple
        one-column indexes (e.g. 'name') fields.
        """

        tables = set(self.idl.tables.keys())
        # lookup table indices
        for table, (lt, col, uuid_col) in self.lookup_table.items():
            if table != lt or not col or uuid_col or table not in tables:
                # Just handle simple cases where we are looking up a single
                # column on a single table
                continue
            index_name = idlutils.index_name(col)
            try:
                idx = self.idl.tables[table].rows.index_create(index_name)
            except ValueError:
                LOG.debug("lookup_table index %s.%s already exists", table,
                          index_name)
            else:
                idx.add_column(col)
                LOG.debug("Created lookup_table index %s.%s", table,
                          index_name)
            tables.remove(table)

        # Simple ovsdb-schema indices
        for table in self.idl.tables.values():
            if table.name not in tables:
                continue
            col = idlutils.get_index_column(table)
            if not col:
                continue
            index_name = idlutils.index_name(col)
            try:
                idx = table.rows.index_create(index_name)
            except ValueError:
                LOG.debug("schema index %s.%s already exists", table,
                          index_name)
            else:
                idx.add_column(col)
                LOG.debug("Created schema index %s.%s", table.name, index_name)
            tables.remove(table.name)

    def start_connection(self, connection):
        try:
            self.ovsdb_connection.start()
        except Exception as e:
            connection_exception = exceptions.OvsdbConnectionUnavailable(
                db_schema=self.schema, error=e)
            LOG.exception(connection_exception)
            raise connection_exception from e

    def restart_connection(self):
        self.ovsdb_connection.stop()
        self.ovsdb_connection.start()

    @property
    def idl(self):
        return self.ovsdb_connection.idl

    @property
    def tables(self):
        return self.idl.tables

    _tables = tables

    def create_transaction(self, check_error=False, log_errors=True, **kwargs):
        return transaction.Transaction(
            self, self.ovsdb_connection,
            self.ovsdb_connection.timeout,
            check_error, log_errors)

    def db_create(self, table, **col_values):
        return cmd.DbCreateCommand(self, table, **col_values)

    def db_create_row(self, table, **col_values):
        return cmd.DbCreateCommand(self, table, _as_row=True, **col_values)

    def db_destroy(self, table, record):
        return cmd.DbDestroyCommand(self, table, record)

    def db_set(self, table, record, *col_values):
        return cmd.DbSetCommand(self, table, record, *col_values)

    def db_add(self, table, record, column, *values):
        return cmd.DbAddCommand(self, table, record, column, *values)

    def db_clear(self, table, record, column):
        return cmd.DbClearCommand(self, table, record, column)

    def db_get(self, table, record, column):
        return cmd.DbGetCommand(self, table, record, column)

    def db_list(self, table, records=None, columns=None, if_exists=False):
        return cmd.DbListCommand(self, table, records, columns, if_exists)

    def db_list_rows(self, table, records=None, if_exists=False):
        return cmd.DbListCommand(self, table, records, columns=None, row=True,
                                 if_exists=if_exists)

    def db_find(self, table, *conditions, **kwargs):
        return cmd.DbFindCommand(self, table, *conditions, **kwargs)

    def db_find_rows(self, table, *conditions, **kwargs):
        return cmd.DbFindCommand(self, table, *conditions, row=True, **kwargs)

    def db_remove(self, table, record, column, *values, **keyvalues):
        return cmd.DbRemoveCommand(self, table, record, column,
                                   *values, **keyvalues)

    def lookup(self, table, record, default=_NO_DEFAULT, timeout=None,
               notify_handler=None):
        """Search for a record in a table

        If timeout and notify_handler of type ``row_event.RowEventHandler``
        are passed, in case the record is not present in the selected table,
        the method creates an event, waiting for this record (UUID), on this
        table and events CREATE and UPDATE. The event returns with the record
        memoized if the record was created or updated.
        """
        try:
            with self.ovsdb_connection.lock:
                return self._lookup(table, record)
        except idlutils.RowNotFound:
            if default is not _NO_DEFAULT:
                return default
            if not notify_handler:
                notify_handler = getattr(self, 'notify_handler', None)
            if not (timeout and notify_handler):
                raise

            wait_event = LookupWaitEvent(self, table, record, timeout)
            notify_handler.watch_event(wait_event)
            if not wait_event.wait():
                LOG.info('Record %s from table %s was not registered in the '
                         'IDL DB cache after %d seconds', record, table,
                         timeout)
                notify_handler.unwatch_event(wait_event)
                raise
            return wait_event.result

    def _lookup(self, table, record):
        if record == "":
            raise TypeError("Cannot look up record by empty string")

        # Handle commands by simply returning its result
        if isinstance(record, cmd.BaseCommand):
            return record.result

        t = self.tables[table]
        if isinstance(record, uuid.UUID):
            try:
                return t.rows[record]
            except KeyError:
                raise idlutils.RowNotFound(table=table, col='uuid',
                                           match=record) from None
        try:
            uuid_ = uuid.UUID(record)
            return t.rows[uuid_]
        except ValueError:
            # Not a UUID string, continue lookup by other means
            pass
        except KeyError:
            # If record isn't found by UUID , go ahead and look up by the table
            pass

        if not self.lookup_table:
            raise idlutils.RowNotFound(table=table, col='record',
                                       match=record)
        # NOTE (twilson) This is an approximation of the db-ctl implementation
        # that allows a partial table, assuming that if a table has a single
        # index, that we should be able to do a lookup by it.
        rl = self.lookup_table.get(
            table,
            idlutils.RowLookup(table, idlutils.get_index_column(t), None))
        # no table means uuid only, no column means lookup table has one row
        if rl.table is None:
            raise idlutils.RowNotFound(table=table, col='uuid', match=record)
        if rl.column is None:
            if t.max_rows == 1:
                return next(iter(t.rows.values()))
            raise idlutils.RowNotFound(table=table, col='uuid', match=record)
        row = idlutils.row_by_value(self, rl.table, rl.column, record)
        if rl.uuid_column:
            rows = getattr(row, rl.uuid_column)
            if len(rows) != 1:
                raise idlutils.RowNotFound(table=table, col='record',
                                           match=record)
            row = rows[0]
        return row
