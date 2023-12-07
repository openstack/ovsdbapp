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
from ovsdbapp.schema.ovn_southbound import api
from ovsdbapp.schema.ovn_southbound import commands as cmd


class OvnSbApiIdlImpl(ovs_idl.Backend, api.API):
    schema = 'OVN_Southbound'
    lookup_table = {
        'Chassis': idlutils.RowLookup('Chassis', 'name', None),
        'MAC_Binding': idlutils.RowLookup('MAC_Binding', 'ip', None),
        'Port_Binding': idlutils.RowLookup(
            'Port_Binding', 'logical_port', None
        ),
    }

    def chassis_add(self, chassis, encap_types, encap_ip, may_exist=False,
                    **columns):
        return cmd.ChassisAddCommand(self, chassis, encap_types, encap_ip,
                                     may_exist, **columns)

    def chassis_del(self, chassis, if_exists=False):
        return cmd.ChassisDelCommand(self, chassis, if_exists)

    def chassis_list(self):
        return cmd.ChassisListCommand(self)

    def lsp_bind(self, port, chassis, may_exist=False):
        return cmd.LspBindCommand(self, port, chassis, may_exist)

    def lsp_unbind(self, port, if_exists=False):
        return cmd.LspUnbindCommand(self, port, if_exists)
