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
from ovsdbapp import constants as const
from ovsdbapp.schema.ovn_northbound import api
from ovsdbapp.schema.ovn_northbound import commands as cmd


class OvnNbApiIdlImpl(ovs_idl.Backend, api.API):
    schema = 'OVN_Northbound'
    lookup_table = {
        'Logical_Switch': idlutils.RowLookup('Logical_Switch', 'name', None),
        'Logical_Router': idlutils.RowLookup('Logical_Router', 'name', None),
        'Load_Balancer': idlutils.RowLookup('Load_Balancer', 'name', None),
    }

    def ls_add(self, switch=None, may_exist=False, **columns):
        return cmd.LsAddCommand(self, switch, may_exist, **columns)

    def ls_del(self, switch, if_exists=False):
        return cmd.LsDelCommand(self, switch, if_exists)

    def ls_list(self):
        return cmd.LsListCommand(self)

    def ls_get(self, switch):
        return cmd.LsGetCommand(self, switch)

    def acl_add(self, switch, direction, priority, match, action, log=False,
                may_exist=False, **external_ids):
        return cmd.AclAddCommand(self, switch, direction, priority,
                                 match, action, log, may_exist, **external_ids)

    def acl_del(self, switch, direction=None, priority=None, match=None):
        return cmd.AclDelCommand(self, switch, direction, priority, match)

    def acl_list(self, switch):
        return cmd.AclListCommand(self, switch)

    def lsp_add(self, switch, port, parent_name=None, tag=None,
                may_exist=False, **columns):
        return cmd.LspAddCommand(self, switch, port, parent_name, tag,
                                 may_exist, **columns)

    def lsp_del(self, port, switch=None, if_exists=False):
        return cmd.LspDelCommand(self, port, switch, if_exists)

    def lsp_list(self, switch=None):
        return cmd.LspListCommand(self, switch)

    def lsp_get(self, port):
        return cmd.LspGetCommand(self, port)

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

    def lr_add(self, router=None, may_exist=False, **columns):
        return cmd.LrAddCommand(self, router, may_exist, **columns)

    def lr_del(self, router, if_exists=False):
        return cmd.LrDelCommand(self, router, if_exists)

    def lr_list(self):
        return cmd.LrListCommand(self)

    def lrp_add(self, router, port, mac, networks, peer=None, may_exist=False,
                **columns):
        return cmd.LrpAddCommand(self, router, port, mac, networks,
                                 peer, may_exist, **columns)

    def lrp_del(self, port, router=None, if_exists=False):
        return cmd.LrpDelCommand(self, port, router, if_exists)

    def lrp_list(self, router):
        return cmd.LrpListCommand(self, router)

    def lrp_set_enabled(self, port, is_enabled):
        return cmd.LrpSetEnabledCommand(self, port, is_enabled)

    def lrp_get_enabled(self, port):
        return cmd.LrpGetEnabledCommand(self, port)

    def lr_route_add(self, router, prefix, nexthop, port=None,
                     policy='dst-ip', may_exist=False):
        return cmd.LrRouteAddCommand(self, router, prefix, nexthop, port,
                                     policy, may_exist)

    def lr_route_del(self, router, prefix=None, if_exists=False):
        return cmd.LrRouteDelCommand(self, router, prefix, if_exists)

    def lr_route_list(self, router):
        return cmd.LrRouteListCommand(self, router)

    def lr_nat_add(self, router, nat_type, external_ip, logical_ip,
                   logical_port=None, external_mac=None, may_exist=False):
        return cmd.LrNatAddCommand(
            self, router, nat_type, external_ip, logical_ip, logical_port,
            external_mac, may_exist)

    def lr_nat_del(self, router, nat_type=None, match_ip=None,
                   if_exists=False):
        return cmd.LrNatDelCommand(self, router, nat_type, match_ip, if_exists)

    def lr_nat_list(self, router):
        return cmd.LrNatListCommand(self, router)

    def lb_add(self, lb, vip, ips, protocol=const.PROTO_TCP, may_exist=False,
               **columns):
        return cmd.LbAddCommand(self, lb, vip, ips, protocol, may_exist,
                                **columns)

    def lb_del(self, lb, vip=None, if_exists=False):
        return cmd.LbDelCommand(self, lb, vip, if_exists)

    def lb_list(self):
        return cmd.LbListCommand(self)

    def lr_lb_add(self, router, lb, may_exist=False):
        return cmd.LrLbAddCommand(self, router, lb, may_exist)

    def lr_lb_del(self, router, lb=None, if_exists=False):
        return cmd.LrLbDelCommand(self, router, lb, if_exists)

    def lr_lb_list(self, router):
        return cmd.LrLbListCommand(self, router)

    def ls_lb_add(self, switch, lb, may_exist=False):
        return cmd.LsLbAddCommand(self, switch, lb, may_exist)

    def ls_lb_del(self, switch, lb=None, if_exists=False):
        return cmd.LsLbDelCommand(self, switch, lb, if_exists)

    def ls_lb_list(self, switch):
        return cmd.LsLbListCommand(self, switch)

    def dhcp_options_add(self, cidr, **external_ids):
        return cmd.DhcpOptionsAddCommand(self, cidr, **external_ids)

    def dhcp_options_del(self, dhcpopt_uuid):
        return cmd.DhcpOptionsDelCommand(self, dhcpopt_uuid)

    def dhcp_options_list(self):
        return cmd.DhcpOptionsListCommand(self)

    def dhcp_options_get(self, dhcpopt_uuid):
        return cmd.DhcpOptionsGetCommand(self, dhcpopt_uuid)

    def dhcp_options_set_options(self, dhcpopt_uuid, **options):
        return cmd.DhcpOptionsSetOptionsCommand(self, dhcpopt_uuid, **options)

    def dhcp_options_get_options(self, dhcpopt_uuid):
        return cmd.DhcpOptionsGetOptionsCommand(self, dhcpopt_uuid)
