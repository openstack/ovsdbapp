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
from ovsdbapp.schema.ovn_northbound import api
from ovsdbapp.schema.ovn_northbound import commands as cmd

LOG = logging.getLogger(__name__)


class OvnNbApiIdlImpl(ovs_idl.Backend, api.API):
    schema = 'OVN_Northbound'
    ovsdb_connection = None
    lookup_table = {
        'Logical_Switch': idlutils.RowLookup('Logical_Switch', 'name', None),
    }

    def __init__(self, connection):
        super(OvnNbApiIdlImpl, self).__init__()
        try:
            if OvnNbApiIdlImpl.ovsdb_connection is None:
                OvnNbApiIdlImpl.ovsdb_connection = connection
            OvnNbApiIdlImpl.ovsdb_connection.start()
        except Exception as e:
            connection_exception = exceptions.OvsdbConnectionUnavailable(
                db_schema=self.schema, error=e)
            LOG.exception(connection_exception)
            raise connection_exception

    @property
    def idl(self):
        return OvnNbApiIdlImpl.ovsdb_connection.idl

    @property
    def tables(self):
        return self.idl.tables

    # NOTE(twilson) _tables is for legacy code, but it has always been used
    # outside the Idl API implementions
    _tables = tables

    def create_transaction(self, check_error=False, log_errors=True, **kwargs):
        return transaction.Transaction(
            self, OvnNbApiIdlImpl.ovsdb_connection,
            OvnNbApiIdlImpl.ovsdb_connection.timeout,
            check_error, log_errors)

    def ls_add(self, switch=None, may_exist=False, **columns):
        return cmd.LsAddCommand(self, switch, may_exist, **columns)

    def ls_del(self, switch, if_exists=False):
        return cmd.LsDelCommand(self, switch, if_exists)

    def ls_list(self):
        return cmd.LsListCommand(self)

    def acl_add(self, switch, direction, priority, match, action, log=False,
                may_exist=False, **external_ids):
        return cmd.AclAddCommand(self, switch, direction, priority,
                                 match, action, log, may_exist, **external_ids)

    def acl_del(self, switch, direction=None, priority=None, match=None):
        return cmd.AclDelCommand(self, switch, direction, priority, match)

    def acl_list(self, switch):
        return cmd.AclListCommand(self, switch)

    def lsp_add(self, switch, port, parent=None, tag=None, may_exist=False,
                **columns):
        return cmd.LspAddCommand(self, switch, port, parent, tag, may_exist,
                                 **columns)

    def lsp_del(self, port, switch=None, if_exists=False):
        return cmd.LspDelCommand(self, port, switch, if_exists)

    def lsp_list(self, switch=None):
        return cmd.LspListCommand(self, switch)

    def lsp_get_parent(self, port):
        return cmd.LspGetParentCommand(self, port)

    def lsp_get_tag(self, port):
        # NOTE (twilson) tag can be unassigned for a while after setting
        return cmd.LspGetTagCommand(self, port)

    def lsp_set_addresses(self, port, addresses):
        return cmd.LspSetAddressesCommand(self, port, addresses)

    def lsp_get_addresses(self, port):
        return cmd.LspGetAddressesCommand(self, port)

    def lsp_set_port_security(self, port, addresses):
        return cmd.LspSetPortSecurityCommand(self, port, addresses)

    def lsp_get_port_security(self, port):
        return cmd.LspGetPortSecurityCommand(self, port)

    def lsp_get_up(self, port):
        return cmd.LspGetUpCommand(self, port)

    def lsp_set_enabled(self, port, is_enabled):
        return cmd.LspSetEnabledCommand(self, port, is_enabled)

    def lsp_get_enabled(self, port):
        return cmd.LspGetEnabledCommand(self, port)

    def lsp_set_type(self, port, port_type):
        return cmd.LspSetTypeCommand(self, port, port_type)

    def lsp_get_type(self, port):
        return cmd.LspGetTypeCommand(self, port)

    def lsp_set_options(self, port, **options):
        return cmd.LspSetOptionsCommand(self, port, **options)

    def lsp_get_options(self, port):
        return cmd.LspGetOptionsCommand(self, port)

    def lsp_set_dhcpv4_options(self, port, dhcpopt_uuids):
        return cmd.LspSetDhcpV4OptionsCommand(self, port, dhcpopt_uuids)

    def lsp_get_dhcpv4_options(self, port):
        return cmd.LspGetDhcpV4OptionsCommand(self, port)

    def dhcp_options_add(self, cidr, **external_ids):
        return cmd.DhcpOptionsAddCommand(self, cidr, **external_ids)

    def dhcp_options_del(self, dhcpopt_uuid):
        return cmd.DhcpOptionsDelCommand(self, dhcpopt_uuid)

    def dhcp_options_list(self):
        return cmd.DhcpOptionsListCommand(self)

    def dhcp_options_set_options(self, dhcpopt_uuid, **options):
        return cmd.DhcpOptionsSetOptionsCommand(self, dhcpopt_uuid, **options)

    def dhcp_options_get_options(self, dhcpopt_uuid):
        return cmd.DhcpOptionsGetOptionsCommand(self, dhcpopt_uuid)
