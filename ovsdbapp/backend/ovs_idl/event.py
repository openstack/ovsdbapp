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

from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import event as ovsdb_event

LOG = logging.getLogger(__name__)


class RowEvent(ovsdb_event.RowEvent):  # pylint: disable=abstract-method
    def match_fn(self, event, row, old):
        """User-overridable custom matching function

        This method takes the same arguments as the RowEvent API call
        `matches` and allows for more complex matching criteria. This
        method will apply additional checks to those specified in the
        creation of the RowEvent
        """
        return True

    def base_match(self, event, row, old):
        if self.conditions and not idlutils.row_match(row, self.conditions):
            return False
        if self.old_conditions:
            if not old:
                return False
            try:
                if not idlutils.row_match(old, self.old_conditions):
                    return False
            except (KeyError, AttributeError):
                # Its possible that old row may not have all columns in it
                return False
        return True

    def matches(self, event, row, old=None):
        if event not in self.events:
            return False
        if row._table.name != self.table:
            return False
        if not self.base_match(event, row, old):
            return False
        if not self.match_fn(event, row, old):
            return False
        LOG.debug("Matched %s: %r to row=%s old=%s", event.upper(), self,
                  idlutils.row2str(row), idlutils.row2str(old) if old else '')
        return True


class WaitEvent(RowEvent, ovsdb_event.WaitEvent):
    pass


class RowEventHandler(ovsdb_event.RowEventHandler):
    def notify(self, event, row, updates=None):
        row = idlutils.frozen_row(row)
        super().notify(event, row, updates)
