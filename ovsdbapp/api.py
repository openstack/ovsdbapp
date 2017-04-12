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

import six


@six.add_metaclass(abc.ABCMeta)
class Command(object):
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


@six.add_metaclass(abc.ABCMeta)
class Transaction(object):
    @abc.abstractmethod
    def commit(self):
        """Commit the transaction to OVSDB"""

    @abc.abstractmethod
    def add(self, command):
        """Append an OVSDB operation to the transaction"""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, tb):
        if exc_type is None:
            self.result = self.commit()


@six.add_metaclass(abc.ABCMeta)
class API(object):
    def __init__(self):
        self._nested_txn = None

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
    def transaction(self, check_error=False, log_errors=True, **kwargs):
        """Create a transaction context.

        :param check_error: Allow the transaction to raise an exception?
        :type check_error:  bool
        :param log_errors:  Log an error if the transaction fails?
        :type log_errors:   bool
        :returns: Either a new transaction or an existing one.
        :rtype: :class:`Transaction`
        """
        if self._nested_txn:
            yield self._nested_txn
        else:
            with self.create_transaction(
                    check_error, log_errors, **kwargs) as txn:
                self._nested_txn = txn
                try:
                    yield txn
                finally:
                    self._nested_txn = None
