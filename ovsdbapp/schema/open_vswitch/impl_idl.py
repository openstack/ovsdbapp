# Copyright (c) 2017 Red Hat Inc.
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

from ovsdbapp.backend import ovs_idl
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.backend.ovs_idl import transaction
from ovsdbapp import exceptions
from ovsdbapp.schema.open_vswitch import api
from ovsdbapp.schema.open_vswitch import commands as cmd

LOG = logging.getLogger(__name__)


class VswitchdInterfaceAddException(exceptions.OvsdbAppException):
    message = "Failed to add interfaces: %(ifaces)s"


class OvsVsctlTransaction(transaction.Transaction):
    def pre_commit(self, txn):
        self.api._ovs.increment('next_cfg')
        txn.expected_ifaces = set()

    def post_commit(self, txn):
        super().post_commit(txn)
        # ovs-vsctl only logs these failures and does not return nonzero
        try:
            self.do_post_commit(txn)
        except Exception:
            LOG.exception("Post-commit checks failed")

    def do_post_commit(self, txn):
        next_cfg = txn.get_increment_new_value()
        while not self.timeout_exceeded():
            self.api.idl.run()
            if self.vswitchd_has_completed(next_cfg):
                failed = self.post_commit_failed_interfaces(txn)
                if failed:
                    raise VswitchdInterfaceAddException(
                        ifaces=", ".join(failed))
                break
            self.ovsdb_connection.poller.timer_wait(
                self.time_remaining() * 1000)
            self.api.idl.wait(self.ovsdb_connection.poller)
            self.ovsdb_connection.poller.block()
        else:
            raise exceptions.TimeoutException(
                commands=self.commands, timeout=self.timeout,
                cause='vsctl transaction did not end')

    def post_commit_failed_interfaces(self, txn):
        failed = []
        for iface_uuid in txn.expected_ifaces:
            uuid = txn.get_insert_uuid(iface_uuid)
            if uuid:
                ifaces = self.api.idl.tables['Interface']
                iface = ifaces.rows.get(uuid)
                if iface and (not iface.ofport or iface.ofport == -1):
                    failed.append(iface.name)
        return failed

    def vswitchd_has_completed(self, next_cfg):
        return self.api._ovs.cur_cfg >= next_cfg


class OvsdbIdl(ovs_idl.Backend, api.API):
    schema = 'Open_vSwitch'
    lookup_table = idlutils._LOOKUP_TABLE

    @property
    def connection(self):
        return self.ovsdb_connection

    @property
    def _ovs(self):
        return list(self._tables['Open_vSwitch'].rows.values())[0]

    def create_transaction(self, check_error=False, log_errors=True, **kwargs):
        return OvsVsctlTransaction(self, self.ovsdb_connection,
                                   check_error=check_error,
                                   log_errors=log_errors)

    def add_manager(self, connection_uri):
        return cmd.AddManagerCommand(self, connection_uri)

    def get_manager(self):
        return cmd.GetManagerCommand(self)

    def remove_manager(self, connection_uri):
        return cmd.RemoveManagerCommand(self, connection_uri)

    def add_br(self, name, may_exist=True, datapath_type=None):
        return cmd.AddBridgeCommand(self, name, may_exist, datapath_type)

    def del_br(self, name, if_exists=True):
        return cmd.DelBridgeCommand(self, name, if_exists)

    def br_exists(self, name):
        return cmd.BridgeExistsCommand(self, name)

    def port_to_br(self, name):
        return cmd.PortToBridgeCommand(self, name)

    def iface_to_br(self, name):
        return cmd.InterfaceToBridgeCommand(self, name)

    def list_br(self):
        return cmd.ListBridgesCommand(self)

    def br_get_external_id(self, name, field):
        return cmd.BrGetExternalIdCommand(self, name, field)

    def br_set_external_id(self, name, field, value):
        return cmd.BrSetExternalIdCommand(self, name, field, value)

    def set_controller(self, bridge, controllers):
        return cmd.SetControllerCommand(self, bridge, controllers)

    def del_controller(self, bridge):
        return cmd.DelControllerCommand(self, bridge)

    def get_controller(self, bridge):
        return cmd.GetControllerCommand(self, bridge)

    def set_fail_mode(self, bridge, mode):
        return cmd.SetFailModeCommand(self, bridge, mode)

    def add_port(self, bridge, port, may_exist=True, **interface_attrs):
        return cmd.AddPortCommand(self, bridge, port, may_exist,
                                  **interface_attrs)

    def del_port(self, port, bridge=None, if_exists=True):
        return cmd.DelPortCommand(self, port, bridge, if_exists)

    def list_ports(self, bridge):
        return cmd.ListPortsCommand(self, bridge)

    def list_ifaces(self, bridge):
        return cmd.ListIfacesCommand(self, bridge)

    def iface_get_external_id(self, name, field):
        return cmd.IfaceGetExternalIdCommand(self, name, field)

    def iface_set_external_id(self, name, field, value):
        return cmd.IfaceSetExternalIdCommand(self, name, field, value)
