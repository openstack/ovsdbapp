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

import collections
from collections import abc
import functools
# json is not deprecated
# pylint: disable=deprecated-module
import json
import logging
import os
import sys
import time
import uuid

from ovs.db import idl
from ovs.db import types
from ovs import jsonrpc
from ovs import poller
from ovs import stream

from ovsdbapp import api
from ovsdbapp import exceptions

LOG = logging.getLogger(__name__)

RowLookup = collections.namedtuple('RowLookup',
                                   ['table', 'column', 'uuid_column'])

# Tables with no index in OVSDB and special record lookup rules
_LOOKUP_TABLE = {
    'Controller': RowLookup('Bridge', 'name', 'controller'),
    'Flow_Table': RowLookup('Flow_Table', 'name', None),
    'IPFIX': RowLookup('Bridge', 'name', 'ipfix'),
    'Mirror': RowLookup('Mirror', 'name', None),
    'NetFlow': RowLookup('Bridge', 'name', 'netflow'),
    'Open_vSwitch': RowLookup('Open_vSwitch', None, None),
    'QoS': RowLookup('Port', 'name', 'qos'),
    'Queue': RowLookup(None, None, None),
    'sFlow': RowLookup('Bridge', 'name', 'sflow'),
    'SSL': RowLookup('Open_vSwitch', None, 'ssl'),
}

_NO_DEFAULT = object()


class RowNotFound(exceptions.OvsdbAppException):
    message = "Cannot find %(table)s with %(col)s=%(match)s"


def index_name(*columns):
    assert columns
    return "_".join(sorted(columns))


def index_lookup_all(table, **matches):
    """Find a value in Table by index

    :param table:   The table to search in
    :type table:    ovs.db.schema.TableSchema
    :param matches: The column/value pairs of the index to search
    :type matches:  The types of the columns being matched
    :returns:       An iterator of Row objects
    """
    idx = table.rows.indexes[index_name(*matches.keys())]
    search = table.rows.IndexEntry(**matches)
    return idx.irange(search, search)


def _index_lookup_internal(table, **matches):
    """Returns matching underlying index objects"""

    idx = table.rows.indexes[index_name(*matches.keys())]
    search = table.rows.IndexEntry(**matches)
    return idx.values.irange(search, search)


def index_lookup(table, **matches):
    return next(index_lookup_all(table, **matches))


def table_lookup_all(table, column, match):
    return (r for r in table.rows.values() if getattr(r, column) == match)


def table_lookup(table, column, match):
    return next(table_lookup_all(table, column, match))


def rows_by_value(idl_, table, column, match):
    """Lookup an IDL row in a table by column/value"""
    tab = idl_.tables[table]
    try:
        return index_lookup_all(tab, **{column: match})
    except KeyError:  # no index column
        return table_lookup_all(tab, column, match)


def row_by_value(idl_, table, column, match, default=_NO_DEFAULT):
    try:
        return next(rows_by_value(idl_, table, column, match))
    except StopIteration:
        if default is not _NO_DEFAULT:
            return default
    raise RowNotFound(table=table, col=column, match=match)


def row_by_record(idl_, table, record):
    t = idl_.tables[table]
    try:
        if isinstance(record, uuid.UUID):
            return t.rows[record]
        uuid_ = uuid.UUID(record)
        return t.rows[uuid_]
    except ValueError:
        # Not a UUID string, continue lookup by other means
        pass
    except KeyError as e:
        if sys.platform != 'win32':
            # On Windows the name of the ports is described by the OVS schema:
            # https://tinyurl.com/zk8skhx
            # Is a UUID. (This is due to the fact on Windows port names don't
            # have the 16 chars length limitation as for Linux). Because of
            # this uuid.UUID(record) will not raise a ValueError exception
            # as it happens on Linux and will try to fetch the directly
            # the column instead of using the lookup table. This will raise
            # a KeyError exception on Windows.
            raise RowNotFound(table=table, col='uuid', match=record) from e

    rl = _LOOKUP_TABLE.get(table, RowLookup(table, get_index_column(t), None))
    # no table means uuid only, no column means lookup table only has one row
    if rl.table is None:
        raise ValueError("Table %s can only be queried by UUID") % table
    if rl.column is None:
        return next(iter(t.rows.values()))
    row = row_by_value(idl_, rl.table, rl.column, record)
    if rl.uuid_column:
        rows = getattr(row, rl.uuid_column)
        if len(rows) != 1:
            raise RowNotFound(table=table, col='record', match=record)
        row = rows[0]
    return row


class ExceptionResult(object):
    def __init__(self, ex, tb):
        self.ex = ex
        self.tb = tb


def create_schema_helper(schema):
    """Create a schema helper object based on the provided schema.

    :param schema: The description of the schema
    :type schema: dict or string
    """
    if isinstance(schema, str):
        schema = json.loads(schema)
    return idl.SchemaHelper(None, schema)


def fetch_schema_json(connection, schema_name):
    """Retrieve the schema json from an ovsdb-server

    :param connection: The ovsdb-server connection string
    :type connection: string
    :param schema_name: The schema on the server to pull
    :type schema_name: string
    """
    parsed_connections = parse_connection(connection)

    for c in parsed_connections:
        err, strm = stream.Stream.open_block(
            stream.Stream.open(c))
        if err:
            LOG.error("Unable to open stream to %(conn)s to retrieve schema: "
                      "%(err)s", {'conn': c,
                                  'err': os.strerror(err)})
            continue
        rpc = jsonrpc.Connection(strm)
        req = jsonrpc.Message.create_request('get_schema', [schema_name])
        err, resp = rpc.transact_block(req)
        rpc.close()
        if err:
            LOG.info("Could not retrieve schema from %(conn)s: "
                     "%(err)s", {'conn': c,
                                 'err': os.strerror(err)})
            continue
        if resp.error:
            LOG.error("TRXN error, failed to retrieve schema from %(conn)s: "
                      "%(err)s", {'conn': c,
                                  'err': resp.error})
            continue
        return resp.result
    raise Exception("Could not retrieve schema from %s" % connection)


def get_schema_helper(connection, schema_name):
    """Create a schema helper object by querying an ovsdb-server

    :param connection: The ovsdb-server connection string
    :type connection: string
    :param schema_name: The schema on the server to pull
    :type schema_name: string
    """
    return create_schema_helper(fetch_schema_json(connection, schema_name))


def parse_connection(connection_string):
    """Parse a connection string.

    The connection string must be of the form
    proto:address:port,proto:address:port,...

    The parsing logic here must be identical to the one at
    https://github.com/openvswitch/ovs/blob/master/python/ovs/db/idl.py#L162
    for remote connections.

    :param connection_string: The ovsdb-server connection string
    :type connection_string: string
    """
    return [c.strip() for c in connection_string.split(',')]


def wait_for_change(_idl, timeout=None, seqno=None):
    """Wait for the Idl seqno to change

    :param _idl: The Idl instance
    :type _idl: ovs.db.idl.Idl
    :param timeout: raise a TimeoutException after if timeout > 0/not None
    :type timeout: int (seconds) or None
    """
    if timeout and timeout <= 0:
        timeout = None
    if seqno is None:
        seqno = _idl.change_seqno
    stop = time.time() + timeout if timeout else None
    while _idl.change_seqno == seqno and not _idl.run():
        ovs_poller = poller.Poller()
        _idl.wait(ovs_poller)
        if timeout:
            ovs_poller.timer_wait(timeout * 1000)
        ovs_poller.block()
        if stop and time.time() >= stop:
            raise exceptions.TimeoutException()


def get_column_value(row, col):
    """Retrieve column value from the given row.

    If column's type is optional, the value will be returned as a single
    element instead of a list of length 1.
    """
    if col == '_uuid':
        val = row.uuid
    else:
        val = getattr(row, col)

    # Idl returns lists of Rows where ovs-vsctl returns lists of UUIDs
    if isinstance(val, list) and val:
        if isinstance(val[0], idl.Row):
            val = [v.uuid for v in val]
        col_type = row._table.columns[col].type
        # ovs-vsctl treats lists of 1 as single results
        if col_type.is_optional():
            val = val[0]
    return val


def circular(*items):
    """Circularly iterate over the list of arguments"""
    if not items:
        return
    while True:
        yield from items


def merge_intersection(*sorted_gens):
    """Yield non-duplicate entries in the intersection of pre-sorted generators

    Each sorted iterator will be iterated over in parallel, with values that
    cannot be in all iterators or duplicate values being skipped. Values that
    are in each passed sorted_gen will be yielded. There are never any
    intermediate lists/sets etc. which would require storing everything in
    memory.

    :param sorted_gens pre-sorted iterators
    """
    len_sorted_gens = len(sorted_gens)
    sorted_gens = circular(*sorted_gens)
    try:
        first = next(sorted_gens)
        val = cut_off = next(first)
        matches = 1
    except (StopIteration, IndexError):
        return  # The can't all match if one is empty
    for gen in sorted_gens:
        while True:
            if matches == len_sorted_gens:
                yield val
            try:
                val = next(gen)
            except StopIteration:
                return
            if val >= cut_off:
                break
        if val != cut_off:
            cut_off = val
            matches = 1
        else:
            matches += 1


def condition_index_columns(table, *conditions):
    return [(col, op, match) for col, op, match in conditions
            if op == '=' and col in table.rows.indexes]


def index_condition_match(table, *conditions):
    index_columns = condition_index_columns(table, *conditions)
    if not index_columns:
        return
    # We have to access the underlying index objects and not the results from
    # index_lookup_all() because they are the ones with the proper ordering
    return (table.rows[r.uuid] for r in merge_intersection(
        *[_index_lookup_internal(table, **{col: match})
          for col, op, match in index_columns]))


def condition_match(row, condition):
    """Return whether a condition matches a row

    :param row:       An OVSDB Row
    :param condition: A 3-tuple containing (column, operation, match)
    """
    col, op, match = condition
    val = get_column_value(row, col)

    # both match and val are primitive types, so type can be used for type
    # equality here.
    # NOTE (twilson) the above is a lie--not all string types are the same
    #                I haven't investigated the reason for the patch that
    #                added this code, but for now I check string_types
    if type(match) is not type(val) and not all(
            isinstance(x, str) for x in (match, val)):
        # Types of 'val' and 'match' arguments MUST match in all cases with 2
        # exceptions:
        # - 'match' is an empty list and column's type is optional;
        # - 'value' is an empty and  column's type is optional
        if (not all([match, val]) and
                row._table.columns[col].type.is_optional()):
            # utilize the single elements comparison logic
            if match == []:
                match = None
            elif val == []:
                val = None
        else:
            # no need to process any further
            raise ValueError(
                "Column type and condition operand do not match")

    matched = True

    # TODO(twilson) Implement other operators and type comparisons
    # ovs_lib only uses dict '=' and '!=' searches for now
    if isinstance(match, dict):
        for key in match:
            if op == '=':
                if key not in val or match[key] != val[key]:
                    matched = False
                    break
            elif op == '!=':
                if key not in val or match[key] == val[key]:
                    matched = False
                    break
            else:
                raise NotImplementedError()
    elif isinstance(match, list):
        # According to rfc7047, lists support '=' and '!='
        # (both strict and relaxed). Will follow twilson's dict comparison
        # and implement relaxed version (excludes/includes as per standard)
        if op == "=":
            if not all([val, match]):
                return val == match
            for elem in set(match):
                if elem not in val:
                    matched = False
                    break
        elif op == '!=':
            if not all([val, match]):
                return val != match
            for elem in set(match):
                if elem in val:
                    matched = False
                    break
        else:
            raise NotImplementedError()
    else:
        if op == '=':
            if val != match:
                matched = False
        elif op == '!=':
            if val == match:
                matched = False
        else:
            raise NotImplementedError()
    return matched


def row_match(row, conditions):
    """Return whether the row matches the list of conditions"""
    return all(condition_match(row, cond) for cond in conditions)


def get_index_column(table):
    if len(table.indexes) == 1:
        idx = table.indexes[0]
        if len(idx) == 1:
            return idx[0].name


def db_replace_record(obj):
    """Replace any api.Command objects with their results

    This method should leave obj untouched unless the object contains an
    api.Command object.
    """
    if isinstance(obj, abc.Mapping):
        for k, v in obj.items():
            if isinstance(v, api.Command):
                obj[k] = v.result
    elif (isinstance(obj, abc.Sequence) and
          not isinstance(obj, str)):
        for i, v in enumerate(obj):
            if isinstance(v, api.Command):
                try:
                    obj[i] = v.result
                except TypeError:
                    # NOTE(twilson) If someone passes a tuple, then just return
                    # a tuple with the Commands replaced with their results
                    return type(obj)(getattr(v, "result", v) for v in obj)
    elif isinstance(obj, api.Command):
        obj = obj.result
    return obj


def process_value_for_str(row, col):
    class StrUuid(uuid.UUID):
        """A UUID class that will return a repr of the UUID as the UUID string

        This lets us use the default stringification of lists/dicts to display
        in the format we want without having to generate them ourselves
        """
        __repr__ = uuid.UUID.__str__

        @classmethod
        def from_col(cls, col_src, value):
            if col_src.is_ref():
                return cls(int=value.uuid.int)
            return cls(int=value.int)

    # If we are passed UUID as a column, just return the modified row.uuid
    if col == 'uuid':
        return StrUuid(int=row.uuid.int)

    val = getattr(row, col)
    col_type = row._table.columns[col].type
    if col_type.is_optional():
        try:
            val = val[0]
        except IndexError:
            return []  # Unset optional
    elif col_type.is_map():
        # pylint: disable=unnecessary-lambda-assignment
        _ = k = v = lambda x: x
        if col_type.key.type == types.UuidType:
            k = functools.partial(StrUuid.from_col, col_type.key)
        if col_type.value.type == types.UuidType:
            v = functools.partial(StrUuid.from_col, col_type.value)
        if k == v == _:  # No change needed
            return val
        return {k(x): v(y) for x, y in val.items()}
    elif col_type.is_set():
        if col_type.key.type == types.UuidType:
            return [StrUuid.from_col(col_type.key, v) for v in val]
        return getattr(row, col)
    # optional and non-optional uuid-type columns
    if col_type.key.type == types.UuidType:
        return StrUuid.from_col(col_type.key, val)
    return val


def row2str(row):
    return "{table}({data})".format(
        table=row._table.name,
        data=", ".join("{col!s}={val!r}".format(
            col=c, val=process_value_for_str(row, c))
            for c in ['uuid'] + sorted(row._table.columns) if hasattr(row, c)))


def frozen_row(row):
    """Return a namedtuple representation of a idl.Row object

    Row objects are inherently tied to the transaction processing of the Idl.
    This means that if you have a reference to a Row in one thread, and
    another thread starts a transaction that modifies that row, the Row can
    change w/o you knowing it. This is especially noticeable when using the
    RowEventHandler. It is possible for a Row that is passed to notify() by
    the Idl class to change between being matched and the RowEvent.run()
    method being called. This returns an immutable representation of the row
    by using the same class that custom indexes use for searching. This
    should be safe to pass to other threads.
    """
    return row._table.rows.IndexEntry(
        uuid=row.uuid,
        **{col: getattr(row, col)
           for col in row._table.columns if hasattr(row, col)})


def has_table(idl_, table):
    """Check if a table exists in the IDL schema.

    :param _idl: The Idl instance
    :type _idl: ovs.db.idl.Idl
    :param table: The name of the table to check for
    :type table: str
    :return: True if the table exists, False otherwise
    """
    return table in idl_.tables


def table_has_column(idl_, table, column):
    """Check if a column exists in a specific table of the IDL schema.

    :param _idl: The Idl instance
    :type _idl: ovs.db.idl.Idl
    :param table: The name of the table to check
    :type table: str
    :param column: The name of the column to check for
    :type column: str
    :return: True if the column exists in the table, False otherwise
    """
    return table in idl_.tables and column in idl_.tables[table].columns
