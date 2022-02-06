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


class TableGlobalIsEmpty(idlutils.RowNotFound):
    message = "Table 'Global' is empty"


def get_global_record(api):
    # there should be only one record in 'Global' table
    try:
        return next((r for r in api.tables['Global'].rows.values()))
    except StopIteration as e:
        raise TableGlobalIsEmpty from e


class _ListCommand(cmd.ReadOnlyCommand):
    def run_idl(self, txn):
        table = self.api.tables[self.table_name]
        self.result = [rowview.RowView(r) for r in table.rows.values()]


class AddPsCommand(cmd.AddCommand):
    table_name = 'Physical_Switch'

    def __init__(self, api, pswitch, may_exist=False, **columns):
        super().__init__(api)
        self.pswitch = pswitch
        self.columns = columns
        self.may_exist = may_exist

    def run_idl(self, txn):
        config = get_global_record(self.api)
        pswitch = idlutils.row_by_value(self.api.idl, self.table_name, 'name',
                                        self.pswitch, None)
        if pswitch:
            if self.may_exist:
                self.result = rowview.RowView(pswitch)
                return
            msg = "Physical switch %s exists" % self.pswitch
            raise RuntimeError(msg)
        pswitch = txn.insert(self.api.tables[self.table_name])
        pswitch.name = self.pswitch
        self.set_columns(pswitch, **self.columns)
        config.addvalue('switches', pswitch)
        self.result = pswitch.uuid


class DelPsCommand(cmd.BaseCommand):
    table_name = 'Physical_Switch'

    def __init__(self, api, pswitch, if_exists=False):
        super().__init__(api)
        self.pswitch = pswitch
        self.if_exists = if_exists

    def run_idl(self, txn):
        config = get_global_record(self.api)
        try:
            pswitch = self.api.lookup(self.table_name, self.pswitch)
            config.delvalue('switches', pswitch)
            pswitch.delete()
        except idlutils.RowNotFound as e:
            if self.if_exists:
                return
            msg = "Physical switch %s does not exist" % self.pswitch
            raise RuntimeError(msg) from e


class ListPsCommand(_ListCommand):
    table_name = 'Physical_Switch'


class GetPsCommand(cmd.BaseGetRowCommand):
    table = 'Physical_Switch'


class AddPortCommand(cmd.AddCommand):
    table_name = 'Physical_Port'

    def __init__(self, api, pswitch, port, may_exist=False):
        super().__init__(api)
        self.pswitch = pswitch
        self.port = port
        self.may_exist = may_exist
        self.conditions = [('name', '=', self.port)]

    def run_idl(self, txn):
        pswitch = self.api.lookup('Physical_Switch', self.pswitch)
        port = next((p for p in pswitch.ports
                     if idlutils.row_match(p, self.conditions)), None)
        if port:
            if self.may_exist:
                self.result = rowview.RowView(port)
                return
            msg = "Physical port %s exists in %s" % (self.port, self.pswitch)
            raise RuntimeError(msg)
        port = txn.insert(self.api.tables[self.table_name])
        port.name = self.port
        pswitch.addvalue('ports', port)
        self.result = port.uuid


class DelPortCommand(cmd.BaseCommand):
    table_name = 'Physical_Port'

    def __init__(self, api, pswitch, port, if_exists=False):
        super().__init__(api)
        self.pswitch = pswitch
        self.port = port
        self.if_exists = if_exists
        self.conditions = [('name', '=', self.port)]

    def run_idl(self, txn):
        pswitch = self.api.lookup('Physical_Switch', self.pswitch)
        port = next((p for p in pswitch.ports
                     if idlutils.row_match(p, self.conditions)), None)
        if not port:
            if self.if_exists:
                return
            msg = "Physical port %s does not exist in %s" % (self.port,
                                                             self.pswitch)
            raise RuntimeError(msg)
        pswitch.delvalue('ports', port)
        port.delete()


class ListPortsCommand(cmd.ReadOnlyCommand):
    table_name = 'Physical_Switch'

    def __init__(self, api, pswitch):
        super().__init__(api)
        self.pswitch = pswitch

    def run_idl(self, txn):
        pswitch = self.api.lookup(self.table_name, self.pswitch)
        self.result = [rowview.RowView(port) for port in pswitch.ports]


class GetPortCommand(cmd.BaseGetRowCommand):
    table = 'Physical_Port'


class AddLsCommand(cmd.AddCommand):
    table_name = 'Logical_Switch'

    def __init__(self, api, switch, may_exist=False, **columns):
        super().__init__(api)
        self.switch = switch
        self.columns = columns
        self.may_exist = may_exist

    def run_idl(self, txn):
        switch = idlutils.row_by_value(self.api.idl, self.table_name, 'name',
                                       self.switch, None)
        if switch:
            if self.may_exist:
                self.result = rowview.RowView(switch)
                return
            msg = "Logical switch %s exists" % self.switch
            raise RuntimeError(msg)
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
            entity = self.api.lookup(self.table_name, self.switch)
            entity.delete()
        except idlutils.RowNotFound as e:
            if self.if_exists:
                return
            msg = "Logical switch %s does not exist" % self.switch
            raise RuntimeError(msg) from e


class ListLsCommand(_ListCommand):
    table_name = 'Logical_Switch'


class GetLsCommand(cmd.BaseGetRowCommand):
    table = 'Logical_Switch'


class BindLsCommand(cmd.BaseCommand):
    table_name = 'Physical_Port'

    def __init__(self, api, pswitch, port, vlan, switch):
        super().__init__(api)
        self.pswitch = pswitch
        self.port = port
        self.vlan = vlan
        self.switch = switch
        self.conditions = [('name', '=', self.port)]

    def run_idl(self, txn):
        pswitch = self.api.lookup('Physical_Switch', self.pswitch)
        switch = self.api.lookup('Logical_Switch', self.switch)
        port = next((p for p in pswitch.ports
                     if idlutils.row_match(p, self.conditions)), None)
        if not port:
            raise idlutils.RowNotFound(table=self.table_name,
                                       col='name', match=self.port)
        port.setkey('vlan_bindings', self.vlan, switch.uuid)


class UnbindLsCommand(cmd.BaseCommand):
    table_name = 'Physical_Port'

    def __init__(self, api, pswitch, port, vlan):
        super().__init__(api)
        self.pswitch = pswitch
        self.port = port
        self.vlan = vlan
        self.conditions = [('name', '=', self.port)]

    def run_idl(self, txn):
        pswitch = self.api.lookup('Physical_Switch', self.pswitch)
        port = next((p for p in pswitch.ports
                     if idlutils.row_match(p, self.conditions)), None)
        if not port:
            raise idlutils.RowNotFound(table=self.table_name,
                                       col='name', match=self.port)
        port.delkey('vlan_bindings', self.vlan)


class _ClearMacsCommand(cmd.BaseCommand):
    def __init__(self, api, switch):
        super().__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        switch = self.api.lookup('Logical_Switch', self.switch)
        macs = []

        for table_name in self.table_names:
            macs.extend(idlutils.rows_by_value(self.api.idl,
                                               table_name,
                                               'logical_switch', switch))
        for mac in macs:
            mac.delete()


class ClearLocalMacsCommand(_ClearMacsCommand):
    table_names = ['Ucast_Macs_Local', 'Mcast_Macs_Local']


class ClearRemoteMacsCommand(_ClearMacsCommand):
    table_names = ['Ucast_Macs_Remote', 'Mcast_Macs_Remote']


class _ListMacsCommand(cmd.ReadOnlyCommand):
    def __init__(self, api, switch):
        super().__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        switch = self.api.lookup('Logical_Switch', self.switch)
        self.result = []

        for table_name in self.table_names:
            self.result.append([rowview.RowView(mac)
                                for mac in idlutils.rows_by_value(
                                    self.api.idl, table_name,
                                    'logical_switch', switch)])


class ListLocalMacsCommand(_ListMacsCommand):
    table_names = ['Ucast_Macs_Local', 'Mcast_Macs_Local']


class ListRemoteMacsCommand(_ListMacsCommand):
    table_names = ['Ucast_Macs_Remote', 'Mcast_Macs_Remote']
