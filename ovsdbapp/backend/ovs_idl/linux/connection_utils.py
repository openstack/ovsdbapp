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

import os

from ovsdbapp.backend.ovs_idl.common import base_connection_utils


class WaitQueue(base_connection_utils.WaitQueue):
    def init_alert_notification(self):
        alertpipe = os.pipe()
        # NOTE(ivasilevskaya) python 3 doesn't allow unbuffered I/O.
        # Will get around this constraint by using binary mode.
        self.alertin = os.fdopen(alertpipe[0], 'rb', 0)
        self.alertout = os.fdopen(alertpipe[1], 'wb', 0)

    def alert_notification_consume(self):
        self.alertin.read(1)

    def alert_notify(self):
        self.alertout.write('X'.encode("latin-1"))
        self.alertout.flush()

    @property
    def alert_fileno(self):
        return self.alertin.fileno()
