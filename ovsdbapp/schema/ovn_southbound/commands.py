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


class ChassisAddCommand(cmd.AddCommand):
    table_name = 'Chassis'

    def __init__(self, api, chassis, encap_types, encap_ip, may_exist=False,
                 **columns):
        super().__init__(api)
        self.chassis = chassis
        self.encap_types = encap_types
        self.encap_ip = encap_ip
        self.may_exist = may_exist
        self.columns = columns

    def run_idl(self, txn):
        # ovn-sbctl does a client-side check for duplicate entry, but since
        # there is an index on "name", it will fail if we try to insert a
        # duplicate, so I'm not doing the check unless may_exist is set
        if self.may_exist:
            chassis = idlutils.row_by_value(self.api.idl, self.table_name,
                                            'name', self.chassis)
            if chassis:
                self.result = rowview.RowView(chassis)
                return
        chassis = txn.insert(self.api.tables[self.table_name])
        chassis.name = self.chassis
        encaps = []
        for encap_type in self.encap_types:
            encap = txn.insert(self.api.tables['Encap'])
            encap.type = encap_type
            encap.ip = self.encap_ip
            encap.options = {'csum': 'True'}  # ovn-sbctl silently does this...
            # NOTE(twilson) addvalue seems like it should work, but fails with
            # Chassis table col encaps references nonexistent row error
            # chassis.addvalue('encaps', encap)
            encaps.append(encap)
        chassis.encaps = encaps
        for col, val in self.columns.items():
            setattr(chassis, col, val)
        self.result = chassis.uuid


class ChassisDelCommand(cmd.BaseCommand):
    def __init__(self, api, chassis, if_exists=False):
        super().__init__(api)
        self.chassis = chassis
        self.if_exists = if_exists

    def run_idl(self, txn):
        # ovn-sbctl, unlike ovn-nbctl, only looks up by name and not UUI; going
        # to allow UUID because the lookup is cheaper and should be encouraged
        try:
            chassis = self.api.lookup('Chassis', self.chassis)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise
        for encap in getattr(chassis, 'encaps', []):
            encap.delete()
        chassis.delete()

        try:
            chassis_private = self.api.lookup('Chassis_Private', self.chassis)
            chassis_private.delete()
        except idlutils.RowNotFound:
            pass


class ChassisListCommand(cmd.ReadOnlyCommand):
    def run_idl(self, txn):
        self.result = [rowview.RowView(r)
                       for r in self.api.tables['Chassis'].rows.values()]


class LspBindCommand(cmd.BaseCommand):
    def __init__(self, api, port, chassis, may_exist=False):
        super().__init__(api)
        self.port = port
        self.chassis = chassis
        self.may_exist = may_exist

    def run_idl(self, txn):
        chassis = self.api.lookup('Chassis', self.chassis)
        binding = idlutils.row_by_value(self.api.idl, 'Port_Binding',
                                        'logical_port', self.port)
        if binding.chassis:
            if self.may_exist:
                return
            raise RuntimeError("Port %s already bound to %s" % (self.port,
                                                                self.chassis))
        binding.chassis = chassis


class LspUnbindCommand(cmd.BaseCommand):
    def __init__(self, api, port, if_exists=False):
        super().__init__(api)
        self.port = port
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            binding = idlutils.row_by_value(self.api.idl, 'Port_Binding',
                                            'logical_port', self.port)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise
        binding.chassis = []
