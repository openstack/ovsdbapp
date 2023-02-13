# Copyright (c) 2015 OpenStack Foundation
#
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

from ovsdbapp.backend.ovs_idl import command
from ovsdbapp.backend.ovs_idl import idlutils

LOG = logging.getLogger(__name__)

BaseCommand = command.BaseCommand


class AddManagerCommand(command.AddCommand):
    table_name = 'Manager'

    def __init__(self, api, target):
        super().__init__(api)
        self.target = target

    def run_idl(self, txn):
        row = txn.insert(self.api._tables['Manager'])
        row.target = self.target
        try:
            self.api._ovs.addvalue('manager_options', row)
        except AttributeError:  # OVS < 2.6
            self.api._ovs.verify('manager_options')
            self.api._ovs.manager_options = (
                self.api._ovs.manager_options + [row])
        self.result = row.uuid


class GetManagerCommand(command.ReadOnlyCommand):
    def __init__(self, api):
        super().__init__(api)

    def run_idl(self, txn):
        self.result = [m.target for m in
                       self.api._tables['Manager'].rows.values()]


class RemoveManagerCommand(BaseCommand):
    def __init__(self, api, target):
        super().__init__(api)
        self.target = target

    def run_idl(self, txn):
        try:
            manager = idlutils.row_by_value(self.api.idl, 'Manager', 'target',
                                            self.target)
        except idlutils.RowNotFound as e:
            msg = "Manager with target %s does not exist" % self.target
            LOG.error(msg)
            raise RuntimeError(msg) from e
        try:
            self.api._ovs.delvalue('manager_options', manager)
        except AttributeError:  # OVS < 2.6
            self.api._ovs.verify('manager_options')
            manager_list = self.api._ovs.manager_options
            manager_list.remove(manager)
            self.api._ovs.manager_options = manager_list
        manager.delete()


class AddBridgeCommand(command.AddCommand):
    table_name = 'Bridge'

    def __init__(self, api, name, may_exist, datapath_type):
        super().__init__(api)
        self.name = name
        self.may_exist = may_exist
        self.datapath_type = datapath_type

    def run_idl(self, txn):
        if self.may_exist:
            br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name',
                                       self.name, None)
            if br:
                if self.datapath_type:
                    br.datapath_type = self.datapath_type
                self.result = br.uuid
                return
        row = txn.insert(self.api._tables['Bridge'])
        row.name = self.name
        if self.datapath_type:
            row.datapath_type = self.datapath_type
        try:
            self.api._ovs.addvalue('bridges', row)
        except AttributeError:  # OVS < 2.6
            self.api._ovs.verify('bridges')
            self.api._ovs.bridges = self.api._ovs.bridges + [row]

        # Add the internal bridge port
        cmd = AddPortCommand(self.api, self.name, self.name, self.may_exist)
        cmd.run_idl(txn)

        cmd = command.DbSetCommand(self.api, 'Interface', self.name,
                                   ('type', 'internal'))
        cmd.run_idl(txn)
        self.result = row.uuid


class DelBridgeCommand(BaseCommand):
    def __init__(self, api, name, if_exists):
        super().__init__(api)
        self.name = name
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name',
                                       self.name)
        except idlutils.RowNotFound as e:
            if self.if_exists:
                return
            else:
                msg = "Bridge %s does not exist" % self.name
                LOG.error(msg)
                raise RuntimeError(msg) from e
        # Clean up cached ports/interfaces
        for port in br.ports:
            for interface in port.interfaces:
                interface.delete()
            port.delete()
        try:
            self.api._ovs.delvalue('bridges', br)
        except AttributeError:  # OVS < 2.6
            self.api._ovs.verify('bridges')
            bridges = self.api._ovs.bridges
            bridges.remove(br)
            self.api._ovs.bridges = bridges
        br.delete()


class BridgeExistsCommand(command.ReadOnlyCommand):
    def __init__(self, api, name):
        super().__init__(api)
        self.name = name

    def run_idl(self, txn):
        self.result = bool(idlutils.row_by_value(self.api.idl, 'Bridge',
                                                 'name', self.name, None))


class ListBridgesCommand(command.ReadOnlyCommand):
    def __init__(self, api):
        super().__init__(api)

    def run_idl(self, txn):
        # NOTE (twilson) [x.name for x in rows.values()] if no index
        self.result = [x.name for x in
                       self.api._tables['Bridge'].rows.values()]


class SetControllerCommand(BaseCommand):
    def __init__(self, api, bridge, targets):
        super().__init__(api)
        self.bridge = bridge
        self.targets = targets

    def run_idl(self, txn):
        br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name', self.bridge)
        controllers = []
        for target in self.targets:
            controller = txn.insert(self.api._tables['Controller'])
            controller.target = target
            controllers.append(controller)
        # Don't need to verify because we unconditionally overwrite
        br.controller = controllers


class DelControllerCommand(BaseCommand):
    def __init__(self, api, bridge):
        super().__init__(api)
        self.bridge = bridge

    def run_idl(self, txn):
        br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name', self.bridge)
        br.controller = []


class GetControllerCommand(command.ReadOnlyCommand):
    def __init__(self, api, bridge):
        super().__init__(api)
        self.bridge = bridge

    def run_idl(self, txn):
        br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name', self.bridge)
        self.result = [c.target for c in br.controller]


class SetFailModeCommand(BaseCommand):
    def __init__(self, api, bridge, mode):
        super().__init__(api)
        self.bridge = bridge
        self.mode = mode

    def run_idl(self, txn):
        br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name', self.bridge)
        br.fail_mode = self.mode


class AddPortCommand(command.AddCommand):
    table_name = 'Port'

    def __init__(self, api, bridge, port, may_exist, **interface_attrs):
        super().__init__(api)
        self.bridge = bridge
        self.port = port
        self.may_exist = may_exist
        self.interface_attrs = interface_attrs

    def run_idl(self, txn):
        br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name', self.bridge)
        if self.may_exist:
            port = idlutils.row_by_value(self.api.idl, 'Port', 'name',
                                         self.port, None)
            if port:
                self.result = port.uuid
                return
        port = txn.insert(self.api._tables['Port'])
        port.name = self.port
        try:
            br.addvalue('ports', port)
        except AttributeError:  # OVS < 2.6
            br.verify('ports')
            ports = getattr(br, 'ports', [])
            ports.append(port)
            br.ports = ports

        iface = txn.insert(self.api._tables['Interface'])
        txn.expected_ifaces.add(iface.uuid)
        iface.name = self.port
        self.set_columns(iface, **self.interface_attrs)

        # This is a new port, so it won't have any existing interfaces
        port.interfaces = [iface]
        self.result = port.uuid


class DelPortCommand(BaseCommand):
    def __init__(self, api, port, bridge, if_exists):
        super().__init__(api)
        self.port = port
        self.bridge = bridge
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            port = idlutils.row_by_value(self.api.idl, 'Port', 'name',
                                         self.port)
        except idlutils.RowNotFound as e:
            if self.if_exists:
                return
            msg = "Port %s does not exist" % self.port
            raise RuntimeError(msg) from e
        if self.bridge:
            br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name',
                                       self.bridge)
        else:
            br = next(b for b in self.api._tables['Bridge'].rows.values()
                      if port in b.ports)

        if port not in br.ports and not self.if_exists:
            # TODO(twilson) Make real errors across both implementations
            msg = "Port %(port)s does not exist on %(bridge)s!" % {
                'port': self.port, 'bridge': self.bridge
            }
            LOG.error(msg)
            raise RuntimeError(msg)

        try:
            br.delvalue('ports', port)
        except AttributeError:  # OVS < 2.6
            br.verify('ports')
            ports = br.ports
            ports.remove(port)
            br.ports = ports

        # The interface on the port will be cleaned up by ovsdb-server
        for interface in port.interfaces:
            interface.delete()
        port.delete()


class ListPortsCommand(command.ReadOnlyCommand):
    def __init__(self, api, bridge):
        super().__init__(api)
        self.bridge = bridge

    def run_idl(self, txn):
        br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name', self.bridge)
        self.result = [p.name for p in br.ports if p.name != self.bridge]


class ListIfacesCommand(command.ReadOnlyCommand):
    def __init__(self, api, bridge):
        super().__init__(api)
        self.bridge = bridge

    def run_idl(self, txn):
        br = idlutils.row_by_value(self.api.idl, 'Bridge', 'name', self.bridge)
        self.result = [i.name for p in br.ports if p.name != self.bridge
                       for i in p.interfaces]


class PortToBridgeCommand(command.ReadOnlyCommand):
    def __init__(self, api, name):
        super().__init__(api)
        self.name = name

    def run_idl(self, txn):
        # TODO(twilson) This is expensive!
        # This traversal of all ports could be eliminated by caching the bridge
        # name on the Port's external_id field
        # In fact, if we did that, the only place that uses to_br functions
        # could just add the external_id field to the conditions passed to find
        port = idlutils.row_by_value(self.api.idl, 'Port', 'name', self.name)
        bridges = self.api._tables['Bridge'].rows.values()
        self.result = next(br.name for br in bridges if port in br.ports)


class InterfaceToBridgeCommand(command.ReadOnlyCommand):
    def __init__(self, api, name):
        super().__init__(api)
        self.name = name

    def run_idl(self, txn):
        interface = idlutils.row_by_value(self.api.idl, 'Interface', 'name',
                                          self.name)
        ports = self.api._tables['Port'].rows.values()
        pname = next(
            port for port in ports if interface in port.interfaces)

        bridges = self.api._tables['Bridge'].rows.values()
        self.result = next(br.name for br in bridges if pname in br.ports)


class GetExternalIdCommand(command.ReadOnlyCommand):
    def __init__(self, api, table, name, field):
        super().__init__(api)
        self.table = table
        self.name = name
        self.field = field

    def run_idl(self, txn):
        row = idlutils.row_by_value(
            self.api.idl, self.table, 'name', self.name)
        self.result = row.external_ids[self.field]


class SetExternalIdCommand(BaseCommand):
    def __init__(self, api, table, name, field, value):
        super().__init__(api)
        self.table = table
        self.name = name
        self.field = field
        self.value = value

    def run_idl(self, txn):
        row = idlutils.row_by_value(
            self.api.idl, self.table, 'name', self.name)
        external_ids = getattr(row, 'external_ids', {})
        external_ids[self.field] = self.value
        row.external_ids = external_ids


class BrGetExternalIdCommand(GetExternalIdCommand):
    def __init__(self, api, name, field):
        super().__init__(
            api, 'Bridge', name, field)


class BrSetExternalIdCommand(SetExternalIdCommand):
    def __init__(self, api, name, field, value):
        super().__init__(
            api, 'Bridge', name, field, value)


class IfaceGetExternalIdCommand(GetExternalIdCommand):
    def __init__(self, api, name, field):
        super().__init__(
            api, 'Interface', name, field)


class IfaceSetExternalIdCommand(SetExternalIdCommand):
    def __init__(self, api, name, field, value):
        super().__init__(
            api, 'Interface', name, field, value)
