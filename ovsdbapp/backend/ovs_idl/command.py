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

import logging

from ovsdbapp import api

LOG = logging.getLogger(__name__)


class BaseCommand(api.Command):
    def __init__(self, api):
        self.api = api
        self.result = None

    def execute(self, check_error=False, log_errors=True):
        try:
            with self.api.transaction(check_error, log_errors) as txn:
                txn.add(self)
            return self.result
        except Exception:
            if log_errors:
                LOG.exception("Error executing command")
            if check_error:
                raise

    def post_commit(self, txn):
        pass

    def __str__(self):
        command_info = self.__dict__
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join("%s=%s" % (k, v) for k, v in command_info.items()
                      if k not in ['api', 'result']))
