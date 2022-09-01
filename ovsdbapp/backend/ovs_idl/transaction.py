# Copyright (c) 2017 Red Hat Inc
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
import queue
import time

from ovs.db import idl

from ovsdbapp import api
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import exceptions

LOG = logging.getLogger(__name__)


class Transaction(api.Transaction):
    def __init__(self, api, ovsdb_connection, timeout=None,
                 check_error=False, log_errors=True):
        self.api = api
        self.check_error = check_error
        self.log_errors = log_errors
        self.commands = []
        self.results = queue.Queue(1)
        self.ovsdb_connection = ovsdb_connection
        self.timeout = timeout or ovsdb_connection.timeout

    def __str__(self):
        return ", ".join(str(cmd) for cmd in self.commands)

    def add(self, command):
        """Add a command to the transaction

        returns The command passed as a convenience
        """

        self.commands.append(command)
        return command

    def commit(self):
        self.ovsdb_connection.queue_txn(self)
        try:
            result = self.results.get(timeout=self.timeout)
        except queue.Empty as e:
            raise exceptions.TimeoutException(
                commands=self.commands,
                timeout=self.timeout,
                cause='Result queue is empty') from e
        if isinstance(result, idlutils.ExceptionResult):
            if self.log_errors:
                LOG.error(result.tb)
            if self.check_error:
                raise result.ex
        return result

    def pre_commit(self, txn):
        pass

    def post_commit(self, txn):
        for command in self.commands:
            command.post_commit(txn)

    def do_commit(self):
        self.start_time = time.time()
        attempts = 0
        if not self.commands:
            LOG.debug("There are no commands to commit")
            return []
        while True:
            if attempts > 0 and self.timeout_exceeded():
                raise RuntimeError("OVS transaction timed out")
            attempts += 1
            # TODO(twilson) Make sure we don't loop longer than vsctl_timeout
            seqno = self.api.idl.change_seqno
            txn = idl.Transaction(self.api.idl)
            self.pre_commit(txn)
            for i, command in enumerate(self.commands):
                LOG.debug("Running txn n=%(n)d command(idx=%(idx)s): %(cmd)s",
                          {'idx': i, 'cmd': command, 'n': attempts})
                try:
                    command.run_idl(txn)
                except Exception as e:
                    txn.abort()
                    if self.check_error:
                        raise
                    if self.log_errors:
                        LOG.error("txn n=%(n)d command(idx=%(idx)s): %(cmd)s "
                                  "aborted due to error: %(err)s",
                                  {'idx': i,
                                   'cmd': command,
                                   'n': attempts,
                                   'err': e})
            status = txn.commit_block()
            if status == txn.TRY_AGAIN:
                LOG.debug("OVSDB transaction returned TRY_AGAIN, retrying")
                idlutils.wait_for_change(self.api.idl, self.time_remaining(),
                                         seqno)
                continue
            if status in (txn.ERROR, txn.NOT_LOCKED):
                msg = 'OVSDB Error: '
                if status == txn.NOT_LOCKED:
                    msg += ("The transaction failed because the IDL has "
                            "been configured to require a database lock "
                            "but didn't get it yet or has already lost it")
                else:
                    msg += txn.get_error()

                if self.log_errors:
                    LOG.error(msg)
                if self.check_error:
                    # For now, raise similar error to vsctl/utils.execute()
                    raise RuntimeError(msg)
                return
            if status == txn.ABORTED:
                LOG.debug("Transaction aborted")
                return
            if status == txn.UNCHANGED:
                LOG.debug("Transaction caused no change")
            elif status == txn.SUCCESS:
                self.post_commit(txn)
            else:
                LOG.debug("Transaction returned an unknown status: %s", status)

            return [cmd.result for cmd in self.commands]

    def elapsed_time(self):
        return time.time() - self.start_time

    def time_remaining(self):
        return self.timeout - self.elapsed_time()

    def timeout_exceeded(self):
        return self.elapsed_time() > self.timeout
