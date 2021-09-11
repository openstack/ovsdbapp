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

from ovsdbapp.backend.ovs_idl import command as cmd
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.backend.ovs_idl import rowview


class TsAddCommand(cmd.AddCommand):
    table_name = 'Transit_Switch'

    def __init__(self, api, switch, may_exist=False, **columns):
        super().__init__(api)
        self.switch = switch
        self.columns = columns
        self.may_exist = may_exist

    def run_idl(self, txn):
        # There is requirement for name to be unique
        # (index in ovn-ic-nb.ovsschema)
        switch = idlutils.row_by_value(self.api.idl, self.table_name, 'name',
                                       self.switch, None)
        if switch:
            if self.may_exist:
                self.result = rowview.RowView(switch)
                return
            raise RuntimeError("Transit Switch %s exists" % self.switch)
        switch = txn.insert(self.api.tables[self.table_name])
        switch.name = self.switch
        self.set_columns(switch, **self.columns)
        self.result = switch.uuid


class TsDelCommand(cmd.BaseCommand):
    def __init__(self, api, switch, if_exists=False):
        super().__init__(api)
        self.switch = switch
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            switch = self.api.lookup('Transit_Switch', self.switch)
            switch.delete()
        except idlutils.RowNotFound as e:
            if self.if_exists:
                return
            msg = "Transit Switch %s does not exist" % self.switch
            raise RuntimeError(msg) from e


class TsListCommand(cmd.ReadOnlyCommand):
    def run_idl(self, txn):
        table = self.api.tables['Transit_Switch']
        self.result = [rowview.RowView(r) for r in table.rows.values()]


class TsGetCommand(cmd.BaseGetRowCommand):
    table = 'Transit_Switch'
