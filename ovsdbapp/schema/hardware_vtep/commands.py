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


class AddLsCommand(cmd.AddCommand):
    table_name = 'Logical_Switch'

    def __init__(self, api, switch, may_exist=False, **columns):
        super().__init__(api)
        self.switch = switch
        self.columns = columns
        self.may_exist = may_exist

    def run_idl(self, txn):
        # There is requirement for name to be unique
        # (index in vtep.ovsschema)
        switch = idlutils.row_by_value(self.api.idl, self.table_name, 'name',
                                       self.switch, None)
        if switch:
            if self.may_exist:
                self.result = rowview.RowView(switch)
                return
            raise RuntimeError("Logical Switch %s exists" % self.switch)
        switch = txn.insert(self.api.tables[self.table_name])
        switch.name = self.switch
        self.set_columns(switch, **self.columns)
        self.result = switch.uuid


class DelLsCommand(cmd.BaseCommand):
    table_name = 'Logical_Switch'

    def __init__(self, api, switch, if_exists=False):
        super().__init__(api)
        self.switch = switch
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            switch = self.api.lookup(self.table_name, self.switch)
            switch.delete()
        except idlutils.RowNotFound as e:
            if self.if_exists:
                return
            msg = "Logical Switch %s does not exist" % self.switch
            raise RuntimeError(msg) from e


class ListLsCommand(cmd.ReadOnlyCommand):
    table_name = 'Logical_Switch'

    def run_idl(self, txn):
        table = self.api.tables[self.table_name]
        self.result = [rowview.RowView(r) for r in table.rows.values()]


class GetLsCommand(cmd.BaseGetRowCommand):
    table = 'Logical_Switch'


class BindLsCommand(cmd.BaseCommand):
    def __init__(self, api, pswitch, port, vlan, switch):
        super().__init__(api)
        self.pswitch = pswitch
        self.port = port
        self.vlan = vlan
        self.switch = switch

    def run_idl(self, txn):
        pswitch = self.api.lookup('Physical_Switch', self.pswitch)
        switch = self.api.lookup('Logical_Switch', self.switch)
        port = self.api.lookup('Physical_Port', self.port)

        if port not in pswitch.ports:
            msg = "Port %s not found in %s" % (self.port, self.pswitch)
            raise RuntimeError(msg)

        port.setkey('vlan_bindings', self.vlan, switch.uuid)


class UnbindLsCommand(cmd.BaseCommand):
    def __init__(self, api, pswitch, port, vlan):
        super().__init__(api)
        self.pswitch = pswitch
        self.port = port
        self.vlan = vlan

    def run_idl(self, txn):
        pswitch = self.api.lookup('Physical_Switch', self.pswitch)
        port = self.api.lookup('Physical_Port', self.port)

        if port not in pswitch.ports:
            msg = "Port %s not found in %s" % (self.port, self.pswitch)
            raise RuntimeError(msg)

        port.delkey('vlan_bindings', self.vlan)


class _ClearMacsCommand(cmd.BaseCommand):
    def __init__(self, api, switch):
        super().__init__(api)
        self.switch = switch
        self.local = None

    def run_idl(self, txn):
        switch = self.api.lookup('Logical_Switch', self.switch)
        macs = []

        if self.local:
            table_names = ['Ucast_Macs_Local', 'Mcast_Macs_Local']
        else:
            table_names = ['Ucast_Macs_Remote', 'Mcast_Macs_Remote']

        for table_name in table_names:
            macs.extend(idlutils.rows_by_value(self.api.idl,
                                               table_name,
                                               'logical_switch', switch))
        for mac in macs:
            mac.delete()


class ClearLocalMacsCommand(_ClearMacsCommand):
    def __init__(self, api, switch):
        super().__init__(api, switch)
        self.local = True


class ClearRemoteMacsCommand(_ClearMacsCommand):
    def __init__(self, api, switch):
        super().__init__(api, switch)
        self.local = False
