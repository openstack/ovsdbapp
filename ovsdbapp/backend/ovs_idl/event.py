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
    def matches(self, event, row, old=None):
        if event not in self.events:
            return False
        if row._table.name != self.table:
            return False
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

        LOG.debug("%s : Matched %s, %s, %s %s", self.event_name, self.table,
                  self.events, self.conditions, self.old_conditions)
        return True
