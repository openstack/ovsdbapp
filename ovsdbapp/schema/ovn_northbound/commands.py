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
import re

import netaddr

from ovsdbapp.backend.ovs_idl import command as cmd
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.backend.ovs_idl import rowview
from ovsdbapp import constants as const
from ovsdbapp import utils


class LsAddCommand(cmd.AddCommand):
    table_name = 'Logical_Switch'

    def __init__(self, api, switch=None, may_exist=False, **columns):
        super(LsAddCommand, self).__init__(api)
        self.switch = switch
        self.columns = columns
        self.may_exist = may_exist

    def run_idl(self, txn):
        # There is no requirement for name to be unique, so if a name is
        # specified, we always have to do a lookup since adding it won't
        # fail. If may_exist is set, we just don't do anything when dup'd
        if self.switch:
            sw = idlutils.row_by_value(self.api.idl, self.table_name, 'name',
                                       self.switch, None)
            if sw:
                if self.may_exist:
                    self.result = rowview.RowView(sw)
                    return
                raise RuntimeError("Switch %s exists" % self.switch)
        elif self.may_exist:
            raise RuntimeError("may_exist requires name")
        sw = txn.insert(self.api.tables[self.table_name])
        if self.switch:
            sw.name = self.switch
        else:
            # because ovs.db.idl brokenly requires a changed column
            sw.name = ""
        self.set_columns(sw, **self.columns)
        self.result = sw.uuid


class LsDelCommand(cmd.BaseCommand):
    def __init__(self, api, switch, if_exists=False):
        super(LsDelCommand, self).__init__(api)
        self.switch = switch
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            lswitch = self.api.lookup('Logical_Switch', self.switch)
            lswitch.delete()
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            msg = "Logical Switch %s does not exist" % self.switch
            raise RuntimeError(msg)


class LsListCommand(cmd.BaseCommand):
    def run_idl(self, txn):
        table = self.api.tables['Logical_Switch']
        self.result = [rowview.RowView(r) for r in table.rows.values()]


class LsGetCommand(cmd.BaseGetRowCommand):
    table = 'Logical_Switch'


class AclAddCommand(cmd.AddCommand):
    table_name = 'ACL'

    def __init__(self, api, switch, direction, priority, match, action,
                 log=False, may_exist=False, **external_ids):
        if direction not in ('from-lport', 'to-lport'):
            raise TypeError("direction must be either from-lport or to-lport")
        if not 0 <= priority <= const.ACL_PRIORITY_MAX:
            raise ValueError("priority must be beween 0 and %s, inclusive" % (
                             const.ACL_PRIORITY_MAX))
        if action not in ('allow', 'allow-related', 'drop', 'reject'):
            raise TypeError("action must be allow/allow-related/drop/reject")
        super(AclAddCommand, self).__init__(api)
        self.switch = switch
        self.direction = direction
        self.priority = priority
        self.match = match
        self.action = action
        self.log = log
        self.may_exist = may_exist
        self.external_ids = external_ids

    def acl_match(self, row):
        return (self.direction == row.direction and
                self.priority == row.priority and
                self.match == row.match)

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        acls = [acl for acl in ls.acls if self.acl_match(acl)]
        if acls:
            if self.may_exist:
                self.result = rowview.RowView(acls[0])
                return
            raise RuntimeError("ACL (%s, %s, %s) already exists" % (
                self.direction, self.priority, self.match))
        acl = txn.insert(self.api.tables[self.table_name])
        acl.direction = self.direction
        acl.priority = self.priority
        acl.match = self.match
        acl.action = self.action
        acl.log = self.log
        ls.addvalue('acls', acl)
        for col, value in self.external_ids.items():
            acl.setkey('external_ids', col, value)
        self.result = acl.uuid


class AclDelCommand(cmd.BaseCommand):
    def __init__(self, api, switch, direction=None,
                 priority=None, match=None):
        if (priority is None) != (match is None):
            raise TypeError("Must specify priority and match together")
        if priority is not None and not direction:
            raise TypeError("Cannot specify priority/match without direction")
        super(AclDelCommand, self).__init__(api)
        self.switch = switch
        self.conditions = []
        if direction:
            self.conditions.append(('direction', '=', direction))
            # priority can be 0
            if match:  # and therefor prioroity due to the above check
                self.conditions += [('priority', '=', priority),
                                    ('match', '=', match)]

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        for acl in [a for a in ls.acls
                    if idlutils.row_match(a, self.conditions)]:
            ls.delvalue('acls', acl)
            acl.delete()


class AclListCommand(cmd.BaseCommand):
    def __init__(self, api, switch):
        super(AclListCommand, self).__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        self.result = [rowview.RowView(acl) for acl in ls.acls]


class LspAddCommand(cmd.AddCommand):
    table_name = 'Logical_Switch_Port'

    def __init__(self, api, switch, port, parent_name=None, tag=None,
                 may_exist=False, **columns):
        if tag and not 0 <= tag <= 4095:
            raise TypeError("tag must be 0 to 4095, inclusive")
        if (parent_name is None) != (tag is None):
            raise TypeError("parent_name and tag must be passed together")
        super(LspAddCommand, self).__init__(api)
        self.switch = switch
        self.port = port
        self.parent = parent_name
        self.tag = tag
        self.may_exist = may_exist
        self.columns = columns

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        try:
            lsp = self.api.lookup(self.table_name, self.port)
            if self.may_exist:
                msg = None
                if lsp not in ls.ports:
                    msg = "%s exists, but is not in %s" % (
                        self.port, self.switch)
                if self.parent:
                    if not lsp.parent_name:
                        msg = "%s exists, but has no parent" % self.port
                    # parent_name, being optional, is stored as list
                    if self.parent not in lsp.parent_name:
                        msg = "%s exists with different parent" % self.port
                    if self.tag not in lsp.tag_request:
                        msg = "%s exists with different tag request" % (
                            self.port,)
                elif lsp.parent_name:
                    msg = "%s exists, but with a parent" % self.port

                if msg:
                    raise RuntimeError(msg)
                self.result = rowview.RowView(lsp)
                return
        except idlutils.RowNotFound:
            # This is what we want
            pass
        lsp = txn.insert(self.api.tables[self.table_name])
        lsp.name = self.port
        if self.tag is not None:
            lsp.parent_name = self.parent
            lsp.tag_request = self.tag
        ls.addvalue('ports', lsp)
        self.set_columns(lsp, **self.columns)
        self.result = lsp.uuid


class PortDelCommand(cmd.BaseCommand):
    def __init__(self, api, table, port, parent_table, parent=None,
                 if_exists=False):
        super(PortDelCommand, self).__init__(api)
        self.table = table
        self.port = port
        self.parent_table = parent_table
        self.parent = parent
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            row = self.api.lookup(self.table, self.port)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise RuntimeError("%s does not exist" % self.port)

        # We need to delete the port from its parent
        if self.parent:
            parent = self.api.lookup(self.parent_table, self.parent)
        else:
            parent = next(iter(
                p for p in self.api.tables[self.parent_table].rows.values()
                if row in p.ports), None)
        if not (parent and row in parent.ports):
            raise RuntimeError("%s does not exist in %s" % (
                self.port, self.parent))
        parent.delvalue('ports', row)
        row.delete()


class LspDelCommand(PortDelCommand):
    def __init__(self, api, port, switch=None, if_exists=False):
        super(LspDelCommand, self).__init__(
            api, 'Logical_Switch_Port', port, 'Logical_Switch', switch,
            if_exists)


class LspListCommand(cmd.BaseCommand):
    def __init__(self, api, switch=None):
        super(LspListCommand, self).__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        if self.switch:
            ports = self.api.lookup('Logical_Switch', self.switch).ports
        else:
            ports = self.api.tables['Logical_Switch_Port'].rows.values()
        self.result = [rowview.RowView(r) for r in ports]


class LspGetCommand(cmd.BaseGetRowCommand):
    table = 'Logical_Switch_Port'


class LspGetParentCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetParentCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = next(iter(lsp.parent_name), "")


class LspGetTagCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetTagCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = next(iter(lsp.tag), -1)


class LspSetAddressesCommand(cmd.BaseCommand):
    addr_re = re.compile(
        r'^(router|unknown|dynamic|([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2} .+)$')

    def __init__(self, api, port, addresses):
        for addr in addresses:
            if not self.addr_re.match(addr):
                raise TypeError(
                    "address must be router/unknown/dynamic/ethaddr ipaddr...")
        super(LspSetAddressesCommand, self).__init__(api)
        self.port = port
        self.addresses = addresses

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.addresses = self.addresses


class LspGetAddressesCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetAddressesCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = lsp.addresses


class LspSetPortSecurityCommand(cmd.BaseCommand):
    def __init__(self, api, port, addresses):
        # NOTE(twilson) ovn-nbctl.c does not do any checking of addresses
        # so neither do we
        super(LspSetPortSecurityCommand, self).__init__(api)
        self.port = port
        self.addresses = addresses

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.port_security = self.addresses


class LspGetPortSecurityCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetPortSecurityCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = lsp.port_security


class LspGetUpCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetUpCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        # 'up' is optional, but if not up, it's not up :p
        self.result = next(iter(lsp.up), False)


class LspSetEnabledCommand(cmd.BaseCommand):
    def __init__(self, api, port, is_enabled):
        super(LspSetEnabledCommand, self).__init__(api)
        self.port = port
        self.is_enabled = is_enabled

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.enabled = self.is_enabled


class LspGetEnabledCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetEnabledCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        # enabled is optional, but if not disabled then enabled
        self.result = next(iter(lsp.enabled), True)


class LspSetTypeCommand(cmd.BaseCommand):
    def __init__(self, api, port, port_type):
        super(LspSetTypeCommand, self).__init__(api)
        self.port = port
        self.port_type = port_type

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.type = self.port_type


class LspGetTypeCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetTypeCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = lsp.type


class LspSetOptionsCommand(cmd.BaseCommand):
    def __init__(self, api, port, **options):
        super(LspSetOptionsCommand, self).__init__(api)
        self.port = port
        self.options = options

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.options = self.options


class LspGetOptionsCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetOptionsCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = lsp.options


class LspSetDhcpV4OptionsCommand(cmd.BaseCommand):
    def __init__(self, api, port, dhcpopt_uuid):
        super(LspSetDhcpV4OptionsCommand, self).__init__(api)
        self.port = port
        self.dhcpopt_uuid = dhcpopt_uuid

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.dhcpv4_options = self.dhcpopt_uuid


class LspGetDhcpV4OptionsCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LspGetDhcpV4OptionsCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = next((rowview.RowView(d)
                            for d in lsp.dhcpv4_options), [])


class DhcpOptionsAddCommand(cmd.AddCommand):
    table_name = 'DHCP_Options'

    def __init__(self, api, cidr, **external_ids):
        cidr = netaddr.IPNetwork(cidr)
        super(DhcpOptionsAddCommand, self).__init__(api)
        self.cidr = str(cidr)
        self.external_ids = external_ids

    def run_idl(self, txn):
        dhcpopt = txn.insert(self.api.tables[self.table_name])
        dhcpopt.cidr = self.cidr
        dhcpopt.external_ids = self.external_ids
        self.result = dhcpopt.uuid


class DhcpOptionsDelCommand(cmd.BaseCommand):
    def __init__(self, api, dhcpopt_uuid):
        super(DhcpOptionsDelCommand, self).__init__(api)
        self.dhcpopt_uuid = dhcpopt_uuid

    def run_idl(self, txn):
        dhcpopt = self.api.lookup('DHCP_Options', self.dhcpopt_uuid)
        dhcpopt.delete()


class DhcpOptionsListCommand(cmd.BaseCommand):
    def run_idl(self, txn):
        self.result = [rowview.RowView(r) for
                       r in self.api.tables['DHCP_Options'].rows.values()]


class DhcpOptionsGetCommand(cmd.BaseGetRowCommand):
    table = 'DHCP_Options'


class DhcpOptionsSetOptionsCommand(cmd.BaseCommand):
    def __init__(self, api, dhcpopt_uuid, **options):
        super(DhcpOptionsSetOptionsCommand, self).__init__(api)
        self.dhcpopt_uuid = dhcpopt_uuid
        self.options = options

    def run_idl(self, txn):
        dhcpopt = self.api.lookup('DHCP_Options', self.dhcpopt_uuid)
        dhcpopt.options = self.options


class DhcpOptionsGetOptionsCommand(cmd.BaseCommand):
    def __init__(self, api, dhcpopt_uuid):
        super(DhcpOptionsGetOptionsCommand, self).__init__(api)
        self.dhcpopt_uuid = dhcpopt_uuid

    def run_idl(self, txn):
        dhcpopt = self.api.lookup('DHCP_Options', self.dhcpopt_uuid)
        self.result = dhcpopt.options


class LrAddCommand(cmd.BaseCommand):
    def __init__(self, api, router=None, may_exist=False, **columns):
        super(LrAddCommand, self).__init__(api)
        self.router = router
        self.may_exist = may_exist
        self.columns = columns

    def run_idl(self, txn):
        if self.router:
            try:
                lr = self.api.lookup('Logical_Router', self.router)
                if self.may_exist:
                    self.result = rowview.RowView(lr)
                    return
            except idlutils.RowNotFound:
                pass
        lr = txn.insert(self.api.tables['Logical_Router'])
        lr.name = self.router if self.router else ""
        self.set_columns(lr, **self.columns)
        self.result = lr.uuid

    def post_commit(self, txn):
        real_uuid = txn.get_insert_uuid(self.result)
        if real_uuid:
            row = self.api.tables['Logical_Router'].rows[real_uuid]
            self.result = rowview.RowView(row)


class LrDelCommand(cmd.BaseCommand):
    def __init__(self, api, router, if_exists=False):
        super(LrDelCommand, self).__init__(api)
        self.router = router
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            lr = self.api.lookup('Logical_Router', self.router)
            lr.delete()
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            msg = "Logical Router %s does not exist" % self.router
            raise RuntimeError(msg)


class LrListCommand(cmd.BaseCommand):
    def run_idl(self, txn):
        self.result = [rowview.RowView(r) for
                       r in self.api.tables['Logical_Router'].rows.values()]


class LrpAddCommand(cmd.BaseCommand):
    def __init__(self, api, router, port, mac, networks,
                 peer=None, may_exist=False, **columns):
        self.mac = str(netaddr.EUI(mac, dialect=netaddr.mac_unix_expanded))
        self.networks = [str(netaddr.IPNetwork(net)) for net in networks]
        self.router = router
        self.port = port
        self.peer = peer
        self.may_exist = may_exist
        self.columns = columns
        super(LrpAddCommand, self).__init__(api)

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        try:
            lrp = self.api.lookup('Logical_Router_Port', self.port)
            if self.may_exist:
                msg = None
                if lrp not in lr.ports:
                    msg = "Port %s exists, but is not in router %s" % (
                        self.port, self.router)
                elif netaddr.EUI(lrp.mac) != netaddr.EUI(self.mac):
                    msg = "Port %s exists with different mac" % (self.port)
                elif set(self.networks) != set(lrp.networks):
                    msg = "Port %s exists with different networks" % (
                        self.port)
                elif (not self.peer) != (not lrp.peer) or (
                    self.peer != lrp.peer):
                    msg = "Port %s exists with different peer" % (self.port)
                if msg:
                    raise RuntimeError(msg)
                self.result = rowview.RowView(lrp)
                return
        except idlutils.RowNotFound:
            pass
        lrp = txn.insert(self.api.tables['Logical_Router_Port'])
        # This is what ovn-nbctl does, though the lookup is by uuid or name
        lrp.name = self.port
        lrp.mac = self.mac
        lrp.networks = self.networks
        if self.peer:
            lrp.peer = self.peer
        lr.addvalue('ports', lrp)
        self.set_columns(lrp, **self.columns)
        self.result = lrp.uuid

    def post_commit(self, txn):
        real_uuid = txn.get_insert_uuid(self.result)
        if real_uuid:
            row = self.api.tables['Logical_Router_Port'].rows[real_uuid]
            self.result = rowview.RowView(row)


class LrpDelCommand(PortDelCommand):
    def __init__(self, api, port, router=None, if_exists=False):
        super(LrpDelCommand, self).__init__(
            api, 'Logical_Router_Port', port, 'Logical_Router', router,
            if_exists)


class LrpListCommand(cmd.BaseCommand):
    def __init__(self, api, router):
        super(LrpListCommand, self).__init__(api)
        self.router = router

    def run_idl(self, txn):
        router = self.api.lookup('Logical_Router', self.router)
        self.result = [rowview.RowView(r) for r in router.ports]


class LrpSetEnabledCommand(cmd.BaseCommand):
    def __init__(self, api, port, is_enabled):
        super(LrpSetEnabledCommand, self).__init__(api)
        self.port = port
        self.is_enabled = is_enabled

    def run_idl(self, txn):
        lrp = self.api.lookup('Logical_Router_Port', self.port)
        lrp.enabled = self.is_enabled


class LrpGetEnabledCommand(cmd.BaseCommand):
    def __init__(self, api, port):
        super(LrpGetEnabledCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lrp = self.api.lookup('Logical_Router_Port', self.port)
        # enabled is optional, but if not disabled then enabled
        self.result = next(iter(lrp.enabled), True)


class LrRouteAddCommand(cmd.BaseCommand):
    def __init__(self, api, router, prefix, nexthop, port=None,
                 policy='dst-ip', may_exist=False):
        prefix = str(netaddr.IPNetwork(prefix))
        nexthop = str(netaddr.IPAddress(nexthop))
        super(LrRouteAddCommand, self).__init__(api)
        self.router = router
        self.prefix = prefix
        self.nexthop = nexthop
        self.port = port
        self.policy = policy
        self.may_exist = may_exist

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        for route in lr.static_routes:
            if self.prefix == route.ip_prefix:
                if not self.may_exist:
                    msg = "Route %s already exists on router %s" % (
                        self.prefix, self.router)
                    raise RuntimeError(msg)
                route.nexthop = self.nexthop
                route.policy = self.policy
                if self.port:
                    route.port = self.port
                self.result = rowview.RowView(route)
                return
        route = txn.insert(self.api.tables['Logical_Router_Static_Route'])
        route.ip_prefix = self.prefix
        route.nexthop = self.nexthop
        route.policy = self.policy
        if self.port:
            route.port = self.port
        lr.addvalue('static_routes', route)
        self.result = route.uuid

    def post_commit(self, txn):
        real_uuid = txn.get_insert_uuid(self.result)
        if real_uuid:
            table = self.api.tables['Logical_Router_Static_Route']
            row = table.rows[real_uuid]
            self.result = rowview.RowView(row)


class LrRouteDelCommand(cmd.BaseCommand):
    def __init__(self, api, router, prefix=None, if_exists=False):
        if prefix is not None:
            prefix = str(netaddr.IPNetwork(prefix))
        super(LrRouteDelCommand, self).__init__(api)
        self.router = router
        self.prefix = prefix
        self.if_exists = if_exists

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        if not self.prefix:
            lr.static_routes = []
            return
        for route in lr.static_routes:
            if self.prefix == route.ip_prefix:
                lr.delvalue('static_routes', route)
                # There should only be one possible match
                return

        if not self.if_exists:
            msg = "Route for %s in router %s does not exist" % (
                self.prefix, self.router)
            raise RuntimeError(msg)


class LrRouteListCommand(cmd.BaseCommand):
    def __init__(self, api, router):
        super(LrRouteListCommand, self).__init__(api)
        self.router = router

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        self.result = [rowview.RowView(r) for r in lr.static_routes]


class LrNatAddCommand(cmd.BaseCommand):
    def __init__(self, api, router, nat_type, external_ip, logical_ip,
                 logical_port=None, external_mac=None, may_exist=False):
        if nat_type not in const.NAT_TYPES:
            raise TypeError("nat_type not in %s" % str(const.NAT_TYPES))
        external_ip = str(netaddr.IPAddress(external_ip))
        if nat_type == const.NAT_DNAT:
            logical_ip = str(netaddr.IPAddress(logical_ip))
        else:
            net = netaddr.IPNetwork(logical_ip)
            logical_ip = str(net.ip if net.prefixlen == 32 else net)
        if (logical_port is None) != (external_mac is None):
            msg = "logical_port and external_mac must be passed together"
            raise TypeError(msg)
        if logical_port and nat_type != const.NAT_BOTH:
            msg = "logical_port/external_mac only valid for %s" % (
                const.NAT_BOTH,)
            raise TypeError(msg)
        if external_mac:
            external_mac = str(
                netaddr.EUI(external_mac, dialect=netaddr.mac_unix_expanded))
        super(LrNatAddCommand, self).__init__(api)
        self.router = router
        self.nat_type = nat_type
        self.external_ip = external_ip
        self.logical_ip = logical_ip
        self.logical_port = logical_port or []
        self.external_mac = external_mac or []
        self.may_exist = may_exist

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        if self.logical_port:
            lp = self.api.lookup('Logical_Switch_Port', self.logical_port)
        for nat in lr.nat:
            if ((self.nat_type, self.external_ip, self.logical_ip) ==
                    (nat.type, nat.external_ip, nat.logical_ip)):
                if self.may_exist:
                    nat.logical_port = self.logical_port
                    nat.external_mac = self.external_mac
                    self.result = rowview.RowView(nat)
                    return
                raise RuntimeError("NAT already exists")
        nat = txn.insert(self.api.tables['NAT'])
        nat.type = self.nat_type
        nat.external_ip = self.external_ip
        nat.logical_ip = self.logical_ip
        if self.logical_port:
            # It seems kind of weird that ovn uses a name string instead of
            # a ref to a LSP, especially when ovn-nbctl looks the value up by
            # either name or uuid (and discards the result and store the name).
            nat.logical_port = lp.name
            nat.external_mac = self.external_mac
        lr.addvalue('nat', nat)
        self.result = nat.uuid

    def post_commit(self, txn):
        real_uuid = txn.get_insert_uuid(self.result)
        if real_uuid:
            row = self.api.tables['NAT'].rows[real_uuid]
            self.result = rowview.RowView(row)


class LrNatDelCommand(cmd.BaseCommand):
    def __init__(self, api, router, nat_type=None, match_ip=None,
                 if_exists=False):
        super(LrNatDelCommand, self).__init__(api)
        self.conditions = []
        if nat_type:
            if nat_type not in const.NAT_TYPES:
                raise TypeError("nat_type not in %s" % str(const.NAT_TYPES))
            self.conditions += [('type', '=', nat_type)]
            if match_ip:
                match_ip = str(netaddr.IPAddress(match_ip))
                self.col = ('logical_ip' if nat_type == const.NAT_SNAT
                            else 'external_ip')
                self.conditions += [(self.col, '=', match_ip)]
        elif match_ip:
            raise TypeError("must specify nat_type with match_ip")
        self.router = router
        self.nat_type = nat_type
        self.match_ip = match_ip
        self.if_exists = if_exists

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        found = False
        for nat in [r for r in lr.nat
                    if idlutils.row_match(r, self.conditions)]:
            found = True
            lr.delvalue('nat', nat)
            nat.delete()
            if self.match_ip:
                break
        if self.match_ip and not (found or self.if_exists):
            raise idlutils.RowNotFound(table='NAT', col=self.col,
                                       match=self.match_ip)


class LrNatListCommand(cmd.BaseCommand):
    def __init__(self, api, router):
        super(LrNatListCommand, self).__init__(api)
        self.router = router

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        self.result = [rowview.RowView(r) for r in lr.nat]


class LbAddCommand(cmd.BaseCommand):
    def __init__(self, api, lb, vip, ips, protocol=const.PROTO_TCP,
                 may_exist=False, **columns):
        super(LbAddCommand, self).__init__(api)
        self.lb = lb
        self.vip = utils.normalize_ip_port(vip)
        self.ips = ",".join(utils.normalize_ip_port(ip) for ip in ips)
        self.protocol = protocol
        self.may_exist = may_exist
        self.columns = columns

    def run_idl(self, txn):
        try:
            lb = self.api.lookup('Load_Balancer', self.lb)
            if lb.vips.get(self.vip):
                if not self.may_exist:
                    raise RuntimeError("Load Balancer %s exists" % lb.name)
            # Update load balancer vip
            lb.setkey('vips', self.vip, self.ips)
            lb.protocol = self.protocol
        except idlutils.RowNotFound:
            # New load balancer
            lb = txn.insert(self.api.tables['Load_Balancer'])
            lb.name = self.lb
            lb.protocol = self.protocol
            lb.vips = {self.vip: self.ips}
        self.set_columns(lb, **self.columns)
        self.result = lb.uuid

    def post_commit(self, txn):
        real_uuid = txn.get_insert_uuid(self.result) or self.result
        row = self.api.tables['Load_Balancer'].rows[real_uuid]
        self.result = rowview.RowView(row)


class LbDelCommand(cmd.BaseCommand):
    def __init__(self, api, lb, vip=None, if_exists=False):
        super(LbDelCommand, self).__init__(api)
        self.lb = lb
        self.vip = utils.normalize_ip_port(vip) if vip else vip
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            lb = self.api.lookup('Load_Balancer', self.lb)
            if self.vip:
                if self.vip in lb.vips:
                    if self.if_exists:
                        return
                    lb.delkey('vips', self.vip)
            else:
                lb.delete()
        except idlutils.RowNotFound:
            if not self.if_exists:
                raise


class LbListCommand(cmd.BaseCommand):
    def run_idl(self, txn):
        self.result = [rowview.RowView(r)
                       for r in self.api.tables['Load_Balancer'].rows.values()]


class LrLbAddCommand(cmd.BaseCommand):
    def __init__(self, api, router, lb, may_exist=False):
        super(LrLbAddCommand, self).__init__(api)
        self.router = router
        self.lb = lb
        self.may_exist = may_exist

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        lb = self.api.lookup('Load_Balancer', self.lb)
        if lb in lr.load_balancer:
            if self.may_exist:
                return
            raise RuntimeError("LB %s already exist in router %s" % (
                lb.uuid, lr.uuid))
        lr.addvalue('load_balancer', lb)


class LrLbDelCommand(cmd.BaseCommand):
    def __init__(self, api, router, lb=None, if_exists=False):
        super(LrLbDelCommand, self).__init__(api)
        self.router = router
        self.lb = lb
        self.if_exists = if_exists

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        if not self.lb:
            lr.load_balancer = []
            return
        try:
            lb = self.api.lookup('Load_Balancer', self.lb)
            lr.delvalue('load_balancer', lb)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise


class LrLbListCommand(cmd.BaseCommand):
    def __init__(self, api, router):
        super(LrLbListCommand, self).__init__(api)
        self.router = router

    def run_idl(self, txn):
        lr = self.api.lookup('Logical_Router', self.router)
        self.result = [rowview.RowView(r) for r in lr.load_balancer]


class LsLbAddCommand(cmd.BaseCommand):
    def __init__(self, api, switch, lb, may_exist=False):
        super(LsLbAddCommand, self).__init__(api)
        self.switch = switch
        self.lb = lb
        self.may_exist = may_exist

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        lb = self.api.lookup('Load_Balancer', self.lb)
        if lb in ls.load_balancer:
            if self.may_exist:
                return
            raise RuntimeError("LB %s alseady exist in switch %s" % (
                lb.uuid, ls.uuid))
        ls.addvalue('load_balancer', lb)


class LsLbDelCommand(cmd.BaseCommand):
    def __init__(self, api, switch, lb=None, if_exists=False):
        super(LsLbDelCommand, self).__init__(api)
        self.switch = switch
        self.lb = lb
        self.if_exists = if_exists

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        if not self.lb:
            ls.load_balancer = []
            return
        try:
            lb = self.api.lookup('Load_Balancer', self.lb)
            ls.delvalue('load_balancer', lb)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise


class LsLbListCommand(cmd.BaseCommand):
    def __init__(self, api, switch):
        super(LsLbListCommand, self).__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        self.result = [rowview.RowView(r) for r in ls.load_balancer]
