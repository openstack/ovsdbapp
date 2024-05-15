# Copyright (c) 2017 Red Hat Inc.
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

import collections
from collections import abc
import logging

import ovs.db.idl

from ovsdbapp import api
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.backend.ovs_idl import rowview

LOG = logging.getLogger(__name__)


class BaseCommand(api.Command):
    READ_ONLY = False

    def __init__(self, api):
        self.api = api
        self._result = None

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, value):
        self._result = value

    def execute(self, check_error=False, log_errors=True, **kwargs):
        try:
            if self.READ_ONLY:
                with self.api.ovsdb_connection.lock:
                    self.run_idl(None)
                    return self.result
            with self.api.transaction(check_error, log_errors, **kwargs) as t:
                t.add(self)
            return self.result
        except Exception:
            if log_errors:
                ignoring = "" if check_error else ": IGNORING"
                LOG.exception("Error executing command (%s)%s",
                              type(self).__name__, ignoring)
            if check_error:
                raise

    @classmethod
    def set_column(cls, row, col, val):
        setattr(row, col, idlutils.db_replace_record(val))

    @classmethod
    def set_columns(cls, row, **columns):
        for col, val in columns.items():
            cls.set_column(row, col, val)

    def post_commit(self, txn):
        pass

    def __str__(self):
        command_info = self.__dict__
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join("%s=%s" % (k, v) for k, v in command_info.items()
                      if k not in ['api', 'result']))

    __repr__ = __str__


class ReadOnlyCommand(BaseCommand):
    READ_ONLY = True


class AddCommand(BaseCommand):
    table_name = []  # unhashable, won't be looked up

    def post_commit(self, txn):
        # If get_insert_uuid fails, self.result was not a result of a
        # recent insert. Most likely we are post_commit after a lookup()
        if isinstance(self.result, rowview.RowView):
            return
        if isinstance(self.result, ovs.db.idl.Row):
            row = self.result
        else:
            real_uuid = txn.get_insert_uuid(self.result) or self.result
            # If we have multiple commands in a transation, post_commit can
            # be called even if *this* command caused no change. Theoretically
            # the subclass should have set a UUID/RowView result in that case
            # which is handled above, so raise exception if real_uuid not found
            row = self.api.tables[self.table_name].rows[real_uuid]
        self.result = rowview.RowView(row)


class DbCreateCommand(BaseCommand):
    def __init__(self, api, table, _as_row=False, **columns):
        super().__init__(api)
        self.table = table
        self.columns = columns
        self.row = _as_row

    def run_idl(self, txn):
        row = txn.insert(self.api._tables[self.table])
        self.set_columns(row, **self.columns)
        # This is a temporary row to be used within the transaction
        self.result = row

    def post_commit(self, txn):
        # Replace the temporary row with the post-commit UUID to match vsctl
        u = txn.get_insert_uuid(self.result.uuid)
        if self.row:
            self.result = rowview.RowView(self.api.tables[self.table].rows[u])
        else:
            self.result = u


class DbDestroyCommand(BaseCommand):
    def __init__(self, api, table, record):
        super().__init__(api)
        self.table = table
        self.record = record

    def run_idl(self, txn):
        record = self.api.lookup(self.table, self.record)
        record.delete()


class DbSetCommand(BaseCommand):
    def __init__(self, api, table, record, *col_values, if_exists=False,
                 **columns):
        super().__init__(api)
        self.table = table
        self.record = record
        self.col_values = col_values or columns.items()
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            record = self.api.lookup(self.table, self.record)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise

        for col, val in self.col_values:
            if isinstance(val, abc.Mapping):
                # TODO(twilson) This is to make a unit/functional test that
                # used OrderedDict work. In Python 3.7, insertion order is
                # guaranteed to not change, but I need to verify this is is
                # still even needed
                if isinstance(val, collections.OrderedDict):
                    val = dict(val)
                existing = getattr(record, col, {})
                existing.update(val)
                val = existing
                # Since we are updating certain keys and leaving existing keys
                # but rewriting the whole external_ids column, we must verify()
                record.verify(col)
                # After https://patchwork.ozlabs.org/patch/1254735/ is merged,
                # and common, we should handle dicts with setkey like this:
                # for k, v in val.items():
                #     record.setkey(col, k, v)
            # For non-map columns, we unconditionally overwrite the values that
            # exist, so prior state doesn't matter and we don't need verify()
            self.set_column(record, col, val)


class DbAddCommand(BaseCommand):
    def __init__(self, api, table, record, column, *values):
        super().__init__(api)
        self.table = table
        self.record = record
        self.column = column
        self.values = values

    def run_idl(self, txn):
        record = self.api.lookup(self.table, self.record)
        for value in self.values:
            if isinstance(value, abc.Mapping):
                # We should be doing an add on a 'map' column. If the key is
                # already set, do nothing, otherwise set the key to the value
                # Since this operation depends on the previous value, verify()
                # must be called.
                field = getattr(record, self.column, {})
                for k, v in value.items():
                    if k in field:
                        continue
                    field[k] = v
            else:
                # We should be appending to a 'set' column.
                try:
                    record.addvalue(self.column,
                                    idlutils.db_replace_record(value))
                    continue
                except AttributeError:  # OVS < 2.6
                    field = getattr(record, self.column, [])
                    field.append(value)
            record.verify(self.column)
            self.set_column(record, self.column, field)


class DbClearCommand(BaseCommand):
    def __init__(self, api, table, record, column):
        super().__init__(api)
        self.table = table
        self.record = record
        self.column = column

    def run_idl(self, txn):
        record = self.api.lookup(self.table, self.record)
        # Create an empty value of the column type
        value = type(getattr(record, self.column))()
        setattr(record, self.column, value)


class DbGetCommand(ReadOnlyCommand):
    def __init__(self, api, table, record, column):
        super().__init__(api)
        self.table = table
        self.record = record
        self.column = column

    def run_idl(self, txn):
        record = self.api.lookup(self.table, self.record)
        # TODO(twilson) This feels wrong, but ovs-vsctl returns single results
        # on set types without the list. The IDL is returning them as lists,
        # even if the set has the maximum number of items set to 1. Might be
        # able to inspect the Schema and just do this conversion for that case.
        result = idlutils.get_column_value(record, self.column)
        if isinstance(result, list) and len(result) == 1:
            self.result = result[0]
        else:
            self.result = result


class DbListCommand(ReadOnlyCommand):
    def __init__(self, api, table, records, columns, if_exists, row=False):
        super().__init__(api)
        self.table = table
        self.columns = columns
        self.if_exists = if_exists
        self.records = records
        self.row = row

    def run_idl(self, txn):
        table_schema = self.api._tables[self.table]
        idx = idlutils.get_index_column(table_schema)
        columns = self.columns or list(table_schema.columns.keys()) + ['_uuid']
        # If there's an index for this table, we'll fetch all columns and
        # remove the unwanted ones based on self.records. Otherwise, let's try
        # to get the uuid of the wanted ones which is an O(n^2) operation.
        if not idx and self.records:
            rows = []
            for record in self.records:
                try:
                    rows.append(self.api.idl.lookup(self.table, record))
                except idlutils.RowNotFound:
                    if self.if_exists:
                        continue
                    raise
        else:
            rows = table_schema.rows.values()

        def _match(row):
            elem = getattr(row, idx)
            return elem in self.records

        def _match_remove(row):
            elem = getattr(row, idx)
            found = elem in self.records
            if found:
                records_found.remove(elem)
            return found

        def _match_true(row):
            return True

        records_found = []
        if idx and self.records:
            if self.if_exists:
                match = _match
            else:
                # If we're using the approach of removing the unwanted
                # elements, we'll use a helper list to remove elements as we
                # find them in the DB contents. This will help us identify
                # quickly if there's some record missing to raise a RowNotFound
                # exception later.
                records_found = list(self.records)
                match = _match_remove
        else:
            match = _match_true

        self.result = [
            rowview.RowView(row) if self.row else {
                c: idlutils.get_column_value(row, c)
                for c in columns
            }
            for row in rows if match(row)
        ]

        if records_found:
            raise idlutils.RowNotFound(table=self.table, col=idx,
                                       match=records_found[0])


class DbFindCommand(ReadOnlyCommand):
    def __init__(self, api, table, *conditions, **kwargs):
        super().__init__(api)
        self.table = self.api._tables[table]
        self.conditions = conditions
        self.row = kwargs.get('row', False)
        self.columns = (kwargs.get('columns') or
                        list(self.table.columns.keys()) + ['_uuid'])

    def run_idl(self, txn):
        # reduce search space if we have any indexed column and '=' match
        rows = (idlutils.index_condition_match(self.table, *self.conditions) or
                self.table.rows.values())
        self.result = [
            rowview.RowView(r) if self.row else {
                c: idlutils.get_column_value(r, c)
                for c in self.columns
            }
            for r in rows if idlutils.row_match(r, self.conditions)
        ]


class BaseGetRowCommand(ReadOnlyCommand):
    def __init__(self, api, record):
        super().__init__(api)
        self.record = record

    def run_idl(self, txn):
        self.result = self.api.lookup(self.table, self.record)


class BaseSetOptionsCommand(BaseCommand):
    table = []

    def __init__(self, api, entity, if_exists=False, **options):
        super().__init__(api)
        self.entity = entity
        self.options = options
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            entity = self.api.lookup(self.table, self.entity)
            entity.options = self.options
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise


class BaseGetOptionsCommand(ReadOnlyCommand):
    table = []

    def __init__(self, api, entity, **options):
        super().__init__(api)
        self.entity = entity

    def run_idl(self, txn):
        entity = self.api.lookup(self.table, self.entity)
        self.result = entity.options


class DbRemoveCommand(BaseCommand):
    def __init__(self, api, table, record, column, *values, **keyvalues):
        super().__init__(api)
        self.table = table
        self.record = record
        self.column = column
        self.values = values
        self.keyvalues = keyvalues
        self.if_exists = keyvalues.pop('if_exists', False)

    def run_idl(self, txn):
        try:
            record = self.api.lookup(self.table, self.record)
            if isinstance(getattr(record, self.column), dict):
                for value in self.values:
                    record.delkey(self.column, value)
                for key, value in self.keyvalues.items():
                    record.delkey(self.column, key, value)
            elif isinstance(getattr(record, self.column), list):
                for value in self.values:
                    record.delvalue(self.column, value)
            else:
                value = type(getattr(record, self.column))()
                setattr(record, self.column, value)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise
