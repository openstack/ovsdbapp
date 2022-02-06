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

from ovsdbapp.backend import ovs_idl
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.schema.hardware_vtep import api
from ovsdbapp.schema.hardware_vtep import commands as cmd


class HwVtepApiIdlImpl(ovs_idl.Backend, api.API):
    schema = 'hardware_vtep'
    lookup_table = {
        'Global': idlutils.RowLookup('Global', None, None),
        'Physical_Switch': idlutils.RowLookup('Physical_Switch', 'name', None),
        'Physical_Port': idlutils.RowLookup('Physical_Port', 'name', None),
        'Logical_Switch': idlutils.RowLookup('Logical_Switch', 'name', None),
        'Ucast_Macs_Local': idlutils.RowLookup('Ucast_Macs_Local', None, None),
        'Mcast_Macs_Local': idlutils.RowLookup('Mcast_Macs_Local', None, None),
        'Ucast_Macs_Remote': idlutils.RowLookup('Ucast_Macs_Remote',
                                                None, None),
        'Mcast_Macs_Remote': idlutils.RowLookup('Mcast_Macs_Remote',
                                                None, None),
    }

    def add_ps(self, pswitch, may_exist=False, **columns):
        return cmd.AddPsCommand(self, pswitch, may_exist, **columns)

    def del_ps(self, pswitch, if_exists=False):
        return cmd.DelPsCommand(self, pswitch, if_exists)

    def list_ps(self):
        return cmd.ListPsCommand(self)

    def get_ps(self, pswitch):
        return cmd.GetPsCommand(self, pswitch)

    def add_port(self, pswitch, port, may_exist=False):
        return cmd.AddPortCommand(self, pswitch, port, may_exist)

    def del_port(self, pswitch, port, if_exists=False):
        return cmd.DelPortCommand(self, pswitch, port, if_exists)

    def list_ports(self, pswitch):
        return cmd.ListPortsCommand(self, pswitch)

    def get_port(self, port):
        return cmd.GetPortCommand(self, port)

    def add_ls(self, switch, may_exist=False, **columns):
        return cmd.AddLsCommand(self, switch, may_exist, **columns)

    def del_ls(self, switch, if_exists=False):
        return cmd.DelLsCommand(self, switch, if_exists)

    def list_ls(self):
        return cmd.ListLsCommand(self)

    def get_ls(self, switch):
        return cmd.GetLsCommand(self, switch)

    def bind_ls(self, pswitch, port, vlan, switch):
        return cmd.BindLsCommand(self, pswitch, port, vlan, switch)

    def unbind_ls(self, pswitch, port, vlan):
        return cmd.UnbindLsCommand(self, pswitch, port, vlan)

    def clear_local_macs(self, switch):
        return cmd.ClearLocalMacsCommand(self, switch)

    def clear_remote_macs(self, switch):
        return cmd.ClearRemoteMacsCommand(self, switch)

    def list_local_macs(self, switch):
        return cmd.ListLocalMacsCommand(self, switch)

    def list_remote_macs(self, switch):
        return cmd.ListRemoteMacsCommand(self, switch)
