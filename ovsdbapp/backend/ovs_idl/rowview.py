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

from ovsdbapp.backend.ovs_idl import idlutils


class RowView(object):
    def __init__(self, row):
        self._row = row

    def __getattr__(self, column_name):
        return getattr(self._row, column_name)

    def __eq__(self, other):
        # use other's == since it is likely to be a Row object
        try:
            return other == self._row
        except NotImplementedError:
            return other._row == self._row

    def __hash__(self):
        return self._row.__hash__()

    def __str__(self):
        return idlutils.row2str(self._row)
