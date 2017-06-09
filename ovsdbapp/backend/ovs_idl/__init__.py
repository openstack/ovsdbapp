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

import uuid

from ovsdbapp.backend.ovs_idl import command as cmd
from ovsdbapp.backend.ovs_idl import idlutils

_NO_DEFAULT = object()


class RowView(object):
    def __init__(self, row):
        self._row = row

    def __getattr__(self, column_name):
        return getattr(self._row, column_name)

    def __eq__(self, other):
        # use other's == since it is likely to be a Row object
        try:
            return other == self._row
        except NotImplemented:
            return other._row == self._row

    def __hash__(self):
        return self._row.__hash__()


class Backend(object):
    def db_create(self, table, **col_values):
        return cmd.DbCreateCommand(self, table, **col_values)

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

    def db_find(self, table, *conditions, **kwargs):
        return cmd.DbFindCommand(self, table, *conditions, **kwargs)

    def lookup(self, table, record, default=_NO_DEFAULT):
        try:
            return self._lookup(table, record)
        except idlutils.RowNotFound:
            if default is not _NO_DEFAULT:
                return default
            raise

    def _lookup(self, table, record):
        if record == "":
            raise TypeError("Cannot look up record by empty string")

        t = self.tables[table]
        try:
            if isinstance(record, uuid.UUID):
                return t.rows[record]
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
            return next(iter(t.rows.values()))
        row = idlutils.row_by_value(self, rl.table, rl.column, record)
        if rl.uuid_column:
            rows = getattr(row, rl.uuid_column)
            if len(rows) != 1:
                raise idlutils.RowNotFound(table=table, col='record',
                                           match=record)
            row = rows[0]
        return row
