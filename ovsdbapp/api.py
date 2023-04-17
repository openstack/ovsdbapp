# Copyright (c) 2014 OpenStack Foundation
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

import abc
import contextlib
import threading


class Command(object, metaclass=abc.ABCMeta):
    """An OVSDB command that can be executed in a transaction

    :attr result: The result of executing the command in a transaction
    """

    @abc.abstractmethod
    def execute(self, **transaction_options):
        """Immediately execute an OVSDB command

        This implicitly creates a transaction with the passed options and then
        executes it, returning the value of the executed transaction

        :param transaction_options: Options to pass to the transaction
        """

    @property
    @abc.abstractmethod
    def result(self):
        """Returned value from the command execution"""

    @result.setter
    @abc.abstractmethod
    def result(self, value):
        """Setter of the result property"""


class Transaction(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def commit(self):
        """Commit the transaction to OVSDB"""

    @abc.abstractmethod
    def add(self, command):
        """Append an OVSDB operation to the transaction

        Operation is returned back as a convenience.
        """

    def extend(self, commands):
        """Add multiple OVSDB operations to the transaction

        List of operations is returned back as a convenience.
        """
        return [self.add(command) for command in commands]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, tb):
        if exc_type is None:
            self.result = self.commit()


class API(object, metaclass=abc.ABCMeta):
    def __init__(self, nested_transactions=True):
        # Mapping between a (green)thread and its transaction.
        self._nested_txns = nested_transactions
        self._nested_txns_map = {}

    @abc.abstractmethod
    def create_transaction(self, check_error=False, log_errors=True, **kwargs):
        """Create a transaction

        :param check_error: Allow the transaction to raise an exception?
        :type check_error:  bool
        :param log_errors:  Log an error if the transaction fails?
        :type log_errors:   bool
        :returns: A new transaction
        :rtype: :class:`Transaction`
        """

    @contextlib.contextmanager
    def transaction(self, check_error=False, log_errors=True, nested=True,
                    **kwargs):
        """Create a transaction context.

        :param check_error: Allow the transaction to raise an exception?
        :type check_error:  bool
        :param log_errors:  Log an error if the transaction fails?
        :type log_errors:   bool
        :param nested:      Allow nested transactions be merged into one txn
        :type nested:       bool
        :returns: Either a new transaction or an existing one.
        :rtype: :class:`Transaction`
        """
        # ojbect() is unique, so if we are not nested, this will always result
        # in a KeyError on lookup and so a unique Transaction
        nested = nested and self._nested_txns
        cur_thread_id = threading.get_ident() if nested else object()

        if cur_thread_id in self._nested_txns_map:
            yield self._nested_txns_map[cur_thread_id]
        else:
            with self.create_transaction(
                    check_error, log_errors, **kwargs) as txn:
                self._nested_txns_map[cur_thread_id] = txn
                try:
                    yield txn
                finally:
                    del self._nested_txns_map[cur_thread_id]

    @abc.abstractmethod
    def db_create(self, table, **col_values):
        """Create a command to create new record

        :param table:      The OVS table containing the record to be created
        :type table:       string
        :param col_values: The columns and their associated values
                           to be set after create
        :type col_values:  Dictionary of columns id's and values
        :returns:          :class:`Command` with uuid result
        """

    def db_create_row(self, table, **col_values):
        """Create a command to create new record

        Identical to db_create, but returns a RowView result
        :returns: :class:`Command` with RowView result
        """
        # vif_plug_ovs has a copy of impl_vsctl that doesn't implement this
        raise NotImplementedError

    @abc.abstractmethod
    def db_destroy(self, table, record):
        """Create a command to destroy a record

        :param table:      The OVS table containing the record to be destroyed
        :type table:       string
        :param record:     The record id (name/uuid) to be destroyed
        :type record:      uuid/string
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def db_set(self, table, record, *col_values, if_exists=True, **columns):
        """Create a command to set fields in a record

        :param table:      The OVS table containing the record to be modified
        :type table:       string
        :param record:     The record id (name/uuid) to be modified
        :type table:       string
        :param col_values: The columns and their associated values
        :type col_values:  Tuples of (column, value). Values may be atomic
                           values or unnested sequences/mappings
        :param if_exists:  Do not fail if record does not exist
        :type if_exists:   bool
        :param columns:    The columns and their associated values
                           (mutually exclusive with col_values and col_values
                           is used if available)
        :type columns:     Dictionary of columns names and values
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def db_add(self, table, record, column, *values):
        """Create a command to add a value to a record

        Adds each value or key-value pair to column in record in table. If
        column is a map, then each value will be a dict, otherwise a base type.
        If key already exists in a map column, then the current value is not
        replaced (use the set command to replace an existing value).

        :param table:  The OVS table containing the record to be modified
        :type table:   string
        :param record: The record id (name/uuid) to modified
        :type record:  string
        :param column: The column name to be modified
        :type column:  string
        :param values: The values to be added to the column
        :type values:  The base type of the column. If column is a map, then
                       a dict containing the key name and the map's value type
        :returns:     :class:`Command` with no result
        """

    @abc.abstractmethod
    def db_clear(self, table, record, column):
        """Create a command to clear a field's value in a record

        :param table:  The OVS table containing the record to be modified
        :type table:   string
        :param record: The record id (name/uuid) to be modified
        :type record:  string
        :param column: The column whose value should be cleared
        :type column:  string
        :returns:      :class:`Command` with no result
        """

    @abc.abstractmethod
    def db_get(self, table, record, column):
        """Create a command to return a field's value in a record

        :param table:  The OVS table containing the record to be queried
        :type table:   string
        :param record: The record id (name/uuid) to be queried
        :type record:  string
        :param column: The column whose value should be returned
        :type column:  string
        :returns:      :class:`Command` with the field's value result
        """

    @abc.abstractmethod
    def db_list(self, table, records=None, columns=None, if_exists=False):
        """Create a command to return a list of OVSDB records

        :param table:     The OVS table to query
        :type table:      string
        :param records:   The records to return values from
        :type records:    list of record ids (names/uuids)
        :param columns:   Limit results to only columns, None means all columns
        :type columns:    list of column names or None
        :param if_exists: Do not fail if the record does not exist
        :type if_exists:  bool
        :returns:         :class:`Command` with [{'column', value}, ...] result
        """

    @abc.abstractmethod
    def db_list_rows(self, table, record=None, if_exists=False):
        """Create a command to return a list of OVSDB records

        Identical to db_list, but returns a RowView list result
        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def db_find(self, table, *conditions, **kwargs):
        """Create a command to return find OVSDB records matching conditions

        :param table:     The OVS table to query
        :type table:      string
        :param conditions:The conditions to satisfy the query
        :type conditions: 3-tuples containing (column, operation, match)
                          Type of 'match' parameter MUST be identical to column
                          type
                          Examples:
                              atomic: ('tag', '=', 7)
                              map: ('external_ids' '=', {'iface-id': 'xxx'})
                              field exists?
                                  ('external_ids', '!=', {'iface-id', ''})
                              set contains?:
                                  ('protocols', '{>=}', 'OpenFlow13')
                          See the ovs-vsctl man page for more operations
        :param columns:   Limit results to only columns, None means all columns
        :type columns:    list of column names or None
        :returns:         :class:`Command` with [{'column', value}, ...] result
        """

    @abc.abstractmethod
    def db_find_rows(self, table, *conditions, **kwargs):
        """Create a command to return OVSDB records matching conditions

        Identical to db_find, but returns a list of RowView objects

        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def db_remove(self, table, record, column, *values, **keyvalues):
        """Create a command to delete fields or key-value pairs in a record

        :param table:     The OVS table to query
        :type table:      string
        :param record:    The record id (name/uuid)
        :type record:     string
        :param column:    The column whose value should be deleted
        :type column:     string
        :param values:    In case of list columns, the values to be deleted
                          from the list of values
                          In case of dict columns, the keys to delete
                          regardless of their value
        :type value:      varies depending on column
        :param keyvalues: For dict columns, the keys to delete when the key's
                          value matches the argument value
        :type keyvalues:  values vary depending on column
        :param if_exists: Do not fail if the record does not exist
        :type if_exists:  bool
        :returns:         :class:`Command` with no result
        """
