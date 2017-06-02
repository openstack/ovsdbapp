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

from ovsdbapp.backend import ovs_idl
from ovsdbapp.backend.ovs_idl import command as cmd
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp import constants as const


class AddCommand(cmd.BaseCommand):
    table_name = []  # unhashable, won't be looked up

    def post_commit(self, txn):
        # If get_insert_uuid fails, self.result was not a result of a
        # recent insert. Most likely we are post_commit after a lookup()
        real_uuid = txn.get_insert_uuid(self.result) or self.result
        row = self.api.tables[self.table_name].rows[real_uuid]
        self.result = ovs_idl.RowView(row)


class LsAddCommand(AddCommand):
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
                    self.result = ovs_idl.RowView(sw)
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
        for col, value in self.columns.items():
            setattr(sw, col, value)
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
        self.result = [ovs_idl.RowView(r) for r in table.rows.values()]


class AclAddCommand(AddCommand):
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
                self.result = ovs_idl.RowView(acls[0])
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
        self.result = [ovs_idl.RowView(acl) for acl in ls.acls]


class LspAddCommand(AddCommand):
    table_name = 'Logical_Switch_Port'

    def __init__(self, api, switch, port, parent=None, tag=None,
                 may_exist=False, **columns):
        if tag and not 0 <= tag <= 4095:
            raise TypeError("tag must be 0 to 4095, inclusive")
        if (parent is None) != (tag is None):
            raise TypeError("parent and tag must be passed together")
        super(LspAddCommand, self).__init__(api)
        self.switch = switch
        self.port = port
        self.parent = parent
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
                self.result = ovs_idl.RowView(lsp)
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
        for col, value in self.columns.items():
            setattr(lsp, col, value)
        self.result = lsp.uuid


class LspDelCommand(cmd.BaseCommand):
    def __init__(self, api, port, switch=None, if_exists=False):
        super(LspDelCommand, self).__init__(api)
        self.port = port
        self.switch = switch
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            lsp = self.api.lookup('Logical_Switch_Port', self.port)
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise RuntimeError("%s does not exist" % self.port)

        # We need to delete the port from its switch
        if self.switch:
            sw = self.api.lookup('Logical_Switch', self.switch)
        else:
            sw = next(iter(
                s for s in self.api.tables['Logical_Switch'].rows.values()
                if lsp in s.ports), None)
        if not (sw and lsp in sw.ports):
            raise RuntimeError("%s does not exist in %s" % (
                self.port, self.switch))
        sw.delvalue('ports', lsp)
        lsp.delete()


class LspListCommand(cmd.BaseCommand):
    def __init__(self, api, switch):
        super(LspListCommand, self).__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        sw = self.api.lookup('Logical_Switch', self.switch)
        self.result = [ovs_idl.RowView(r) for r in sw.ports]


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
        self.result = next((ovs_idl.RowView(d)
                            for d in lsp.dhcpv4_options), [])


class DhcpOptionsAddCommand(AddCommand):
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
        self.result = [ovs_idl.RowView(r) for
                       r in self.api.tables['DHCP_Options'].rows.values()]


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
