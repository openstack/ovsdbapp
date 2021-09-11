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
from ovsdbapp.schema.ovn_ic_northbound import api
from ovsdbapp.schema.ovn_ic_northbound import commands as cmd


class OvnIcNbApiIdlImpl(ovs_idl.Backend, api.API):
    schema = 'OVN_IC_Northbound'
    lookup_table = {
        'Transit_Switch': idlutils.RowLookup('Transit_Switch', 'name', None),
    }

    def ts_add(self, switch, may_exist=False, **columns):
        return cmd.TsAddCommand(self, switch, may_exist, **columns)

    def ts_del(self, switch, if_exists=False):
        return cmd.TsDelCommand(self, switch, if_exists)

    def ts_list(self):
        return cmd.TsListCommand(self)

    def ts_get(self, switch):
        return cmd.TsGetCommand(self, switch)
