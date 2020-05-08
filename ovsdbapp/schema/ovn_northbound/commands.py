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


class _AclAddHelper(cmd.AddCommand):
    table_name = 'ACL'

    def __init__(self, api, entity, direction, priority, match, action,
                 log=False, may_exist=False, severity=None, name=None,
                 **external_ids):
        if direction not in ('from-lport', 'to-lport'):
            raise TypeError("direction must be either from-lport or to-lport")
        if not 0 <= priority <= const.ACL_PRIORITY_MAX:
            raise ValueError("priority must be between 0 and %s, inclusive" % (
                             const.ACL_PRIORITY_MAX))
        if action not in ('allow', 'allow-related', 'drop', 'reject'):
            raise TypeError("action must be allow/allow-related/drop/reject")
        super(_AclAddHelper, self).__init__(api)
        self.entity = entity
        self.direction = direction
        self.priority = priority
        self.match = match
        self.action = action
        self.log = log
        self.may_exist = may_exist
        self.severity = severity
        self.name = name
        self.external_ids = external_ids

    def acl_match(self, row):
        return (self.direction == row.direction and
                self.priority == row.priority and
                self.match == row.match)

    def run_idl(self, txn):
        entity = self.api.lookup(self.lookup_table, self.entity)
        acls = [acl for acl in entity.acls if self.acl_match(acl)]
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
        acl.severity = self.severity
        acl.name = self.name
        entity.addvalue('acls', acl)
        for col, value in self.external_ids.items():
            acl.setkey('external_ids', col, value)
        self.result = acl.uuid


class AclAddCommand(_AclAddHelper):
    lookup_table = 'Logical_Switch'

    def __init__(self, api, switch, direction, priority, match, action,
                 log=False, may_exist=False, severity=None, name=None,
                 **external_ids):
        # NOTE: we're overriding the constructor here to not break any
        # existing callers before we introduced Port Groups.
        super(AclAddCommand, self).__init__(api, switch, direction, priority,
                                            match, action, log, may_exist,
                                            severity, name, **external_ids)


class PgAclAddCommand(_AclAddHelper):
    lookup_table = 'Port_Group'


class _AclDelHelper(cmd.BaseCommand):
    def __init__(self, api, entity, direction=None,
                 priority=None, match=None):
        if (priority is None) != (match is None):
            raise TypeError("Must specify priority and match together")
        if priority is not None and not direction:
            raise TypeError("Cannot specify priority/match without direction")
        super(_AclDelHelper, self).__init__(api)
        self.entity = entity
        self.conditions = []
        if direction:
            self.conditions.append(('direction', '=', direction))
            # priority can be 0
            if match:  # and therefore priority due to the above check
                self.conditions += [('priority', '=', priority),
                                    ('match', '=', match)]

    def run_idl(self, txn):
        entity = self.api.lookup(self.lookup_table, self.entity)
        for acl in [a for a in entity.acls
                    if idlutils.row_match(a, self.conditions)]:
            entity.delvalue('acls', acl)
            acl.delete()


class AclDelCommand(_AclDelHelper):
    lookup_table = 'Logical_Switch'

    def __init__(self, api, switch, direction=None,
                 priority=None, match=None):
        # NOTE: we're overriding the constructor here to not break any
        # existing callers before we introduced Port Groups.
        super(AclDelCommand, self).__init__(api, switch, direction, priority,
                                            match)


class PgAclDelCommand(_AclDelHelper):
    lookup_table = 'Port_Group'


class _AclListHelper(cmd.BaseCommand):
    def __init__(self, api, entity):
        super(_AclListHelper, self).__init__(api)
        self.entity = entity

    def run_idl(self, txn):
        entity = self.api.lookup(self.lookup_table, self.entity)
        self.result = [rowview.RowView(acl) for acl in entity.acls]


class AclListCommand(_AclListHelper):
    lookup_table = 'Logical_Switch'


class PgAclListCommand(_AclListHelper):
    lookup_table = 'Port_Group'


class QoSAddCommand(cmd.AddCommand):
    table_name = 'QoS'

    def __init__(self, api, switch, direction, priority, match, rate=None,
                 burst=None, dscp=None, may_exist=False, **columns):
        if direction not in ('from-lport', 'to-lport'):
            raise TypeError("direction must be either from-lport or to-lport")
        if not 0 <= priority <= const.ACL_PRIORITY_MAX:
            raise ValueError("priority must be between 0 and %s, inclusive" %
                             const.ACL_PRIORITY_MAX)
        if rate is not None and not 1 <= rate <= const.QOS_BANDWIDTH_MAX:
            raise ValueError("rate(%s) must be between 1 and %s, inclusive" %
                             rate, const.QOS_BANDWIDTH_MAX)
        if burst is not None and not 1 <= burst <= const.QOS_BANDWIDTH_MAX:
            raise ValueError("burst(%s) must be between 1 and %s, "
                             "inclusive" % burst, const.QOS_BANDWIDTH_MAX)
        if dscp is not None and not 0 <= dscp <= const.QOS_DSCP_MAX:
            raise ValueError("dscp(%s) must be between 0 and %s, inclusive" %
                             dscp, const.QOS_DSCP_MAX)
        if rate is None and dscp is None:
            raise ValueError("One of the rate or dscp must be configured")
        super(QoSAddCommand, self).__init__(api)
        self.switch = switch
        self.direction = direction
        self.priority = priority
        self.match = match
        self.rate = rate
        self.burst = burst
        self.dscp = dscp
        self.may_exist = may_exist
        self.columns = columns

    def qos_match(self, row):
        return (self.direction == row.direction and
                self.priority == row.priority and
                self.match == row.match)

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        qos_rules = [row for row in ls.qos_rules if self.qos_match(row)]
        if qos_rules:
            if self.may_exist:
                self.result = rowview.RowView(qos_rules[0])
                return
            raise RuntimeError("QoS (%s, %s, %s) already exists" % (
                self.direction, self.priority, self.match))
        row = txn.insert(self.api.tables[self.table_name])
        row.direction = self.direction
        row.priority = self.priority
        row.match = self.match
        if self.rate:
            row.setkey('bandwidth', 'rate', self.rate)
            if self.burst:
                row.setkey('bandwidth', 'burst', self.burst)
        if self.dscp is not None:
            row.setkey('action', 'dscp', self.dscp)
        self.set_columns(row, **self.columns)
        ls.addvalue('qos_rules', row)
        self.result = row.uuid


class QoSDelCommand(cmd.BaseCommand):
    def __init__(self, api, switch, direction=None,
                 priority=None, match=None):
        if (priority is None) != (match is None):
            raise TypeError("Must specify priority and match together")
        if priority is not None and not direction:
            raise TypeError("Cannot specify priority/match without direction")
        super(QoSDelCommand, self).__init__(api)
        self.switch = switch
        self.conditions = []
        if direction:
            self.conditions.append(('direction', '=', direction))
            # priority can be 0
            if match:  # and therefor priority due to the above check
                self.conditions += [('priority', '=', priority),
                                    ('match', '=', match)]

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        for row in ls.qos_rules:
            if idlutils.row_match(row, self.conditions):
                ls.delvalue('qos_rules', row)
                row.delete()


class QoSListCommand(cmd.BaseCommand):
    def __init__(self, api, switch):
        super(QoSListCommand, self).__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        self.result = [rowview.RowView(row) for row in ls.qos_rules]


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


class LspGetParentCommand(cmd.ReadOnlyCommand):
    def __init__(self, api, port):
        super(LspGetParentCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = next(iter(lsp.parent_name), "")


class LspGetTagCommand(cmd.ReadOnlyCommand):
    def __init__(self, api, port):
        super(LspGetTagCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = next(iter(lsp.tag), -1)


class LspSetAddressesCommand(cmd.BaseCommand):
    addr_re = re.compile(
        r'^(router|unknown|dynamic|([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}( .+)*)$')

    def __init__(self, api, port, addresses):
        for addr in addresses:
            if not self.addr_re.match(addr):
                raise TypeError(
                    "address (%s) must be router/unknown/dynamic/"
                    "ethaddr[ ipaddr...]" % (addr,))
        super(LspSetAddressesCommand, self).__init__(api)
        self.port = port
        self.addresses = addresses

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.addresses = self.addresses


class LspGetAddressesCommand(cmd.ReadOnlyCommand):
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


class LspGetPortSecurityCommand(cmd.ReadOnlyCommand):
    def __init__(self, api, port):
        super(LspGetPortSecurityCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = lsp.port_security


class LspGetUpCommand(cmd.ReadOnlyCommand):
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


class LspGetEnabledCommand(cmd.ReadOnlyCommand):
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


class LspGetTypeCommand(cmd.ReadOnlyCommand):
    def __init__(self, api, port):
        super(LspGetTypeCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        self.result = lsp.type


class LspSetOptionsCommand(cmd.BaseCommand):
    table = 'Logical_Switch_Port'

    def __init__(self, api, port, **options):
        super(LspSetOptionsCommand, self).__init__(api)
        self.port = port
        self.options = options

    def run_idl(self, txn):
        lsp = self.api.lookup(self.table, self.port)
        lsp.options = self.options


class LspGetOptionsCommand(cmd.ReadOnlyCommand):
    table = 'Logical_Switch_Port'

    def __init__(self, api, port):
        super(LspGetOptionsCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lsp = self.api.lookup(self.table, self.port)
        self.result = lsp.options


class LspSetDhcpV4OptionsCommand(cmd.BaseCommand):
    def __init__(self, api, port, dhcpopt_uuid):
        super(LspSetDhcpV4OptionsCommand, self).__init__(api)
        self.port = port
        self.dhcpopt_uuid = dhcpopt_uuid

    def run_idl(self, txn):
        lsp = self.api.lookup('Logical_Switch_Port', self.port)
        lsp.dhcpv4_options = self.dhcpopt_uuid


class LspGetDhcpV4OptionsCommand(cmd.ReadOnlyCommand):
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


class DhcpOptionsListCommand(cmd.ReadOnlyCommand):
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


class DhcpOptionsGetOptionsCommand(cmd.ReadOnlyCommand):
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


class LrListCommand(cmd.ReadOnlyCommand):
    def run_idl(self, txn):
        self.result = [rowview.RowView(r) for
                       r in self.api.tables['Logical_Router'].rows.values()]


class LrGetCommand(cmd.BaseGetRowCommand):
    table = 'Logical_Router'


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
        gwcs = self.columns.pop('gateway_chassis', [])
        for n, chassis in enumerate(gwcs):
            gwc_name = '%s_%s' % (lrp.name, chassis)
            cmd = GatewayChassisAddCommand(self.api, gwc_name, chassis,
                                           len(gwcs) - n, may_exist=True)
            cmd.run_idl(txn)
            lrp.addvalue('gateway_chassis', cmd.result)
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


class LrpListCommand(cmd.ReadOnlyCommand):
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


class LrpGetEnabledCommand(cmd.ReadOnlyCommand):
    def __init__(self, api, port):
        super(LrpGetEnabledCommand, self).__init__(api)
        self.port = port

    def run_idl(self, txn):
        lrp = self.api.lookup('Logical_Router_Port', self.port)
        # enabled is optional, but if not disabled then enabled
        self.result = next(iter(lrp.enabled), True)


class LrpSetOptionsCommand(LspSetOptionsCommand):
    table = 'Logical_Router_Port'


class LrpGetOptionsCommand(LspGetOptionsCommand):
    table = 'Logical_Router_Port'


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


class LrRouteListCommand(cmd.ReadOnlyCommand):
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
                try:
                    match_ip = str(netaddr.IPAddress(match_ip))
                except ValueError:
                    # logical_ip can be IPNetwork
                    if nat_type == const.NAT_SNAT:
                        match_ip = str(netaddr.IPNetwork(match_ip))
                    else:
                        raise
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


class LrNatListCommand(cmd.ReadOnlyCommand):
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
        except idlutils.RowNotFound:
            if not self.if_exists:
                raise
            return
        if self.vip:
            if self.vip in lb.vips:
                lb.delkey('vips', self.vip)
            elif not self.if_exists:
                raise idlutils.RowNotFound(table='Load_Balancer', col=self.vip,
                                           match=self.lb)
        # Remove load balancer if vips were not provided or no vips are left.
        if not self.vip or not lb.vips:
            lb.delete()


class LbListCommand(cmd.ReadOnlyCommand):
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


class LrLbListCommand(cmd.ReadOnlyCommand):
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


class LsLbListCommand(cmd.ReadOnlyCommand):
    def __init__(self, api, switch):
        super(LsLbListCommand, self).__init__(api)
        self.switch = switch

    def run_idl(self, txn):
        ls = self.api.lookup('Logical_Switch', self.switch)
        self.result = [rowview.RowView(r) for r in ls.load_balancer]


class DnsAddCommand(cmd.AddCommand):
    table_name = 'DNS'

    def __init__(self, api, **columns):
        super(DnsAddCommand, self).__init__(api)
        self.columns = columns

    def run_idl(self, txn):
        dns = txn.insert(self.api.tables[self.table_name])
        # Transaction will not be commited if the row is not initialized with
        # any columns.
        dns.external_ids = {}
        self.set_columns(dns, **self.columns)
        self.result = dns.uuid


class DnsDelCommand(cmd.DbDestroyCommand):
    def __init__(self, api, uuid):
        super(DnsDelCommand, self).__init__(api, 'DNS', uuid)


class DnsGetCommand(cmd.BaseGetRowCommand):
    table = 'DNS'


class DnsListCommand(cmd.ReadOnlyCommand):
    def run_idl(self, txn):
        table = self.api.tables['DNS']
        self.result = [rowview.RowView(r) for r in table.rows.values()]


class DnsSetRecordsCommand(cmd.BaseCommand):
    def __init__(self, api, row_uuid, **records):
        super(DnsSetRecordsCommand, self).__init__(api)
        self.row_uuid = row_uuid
        self.records = records

    def run_idl(self, txn):
        try:
            dns = self.api.lookup('DNS', self.row_uuid)
            dns.records = self.records
        except idlutils.RowNotFound:
            msg = "DNS %s does not exist" % self.row_uuid
            raise RuntimeError(msg)


class DnsSetExternalIdsCommand(cmd.BaseCommand):
    def __init__(self, api, row_uuid, **external_ids):
        super(DnsSetExternalIdsCommand, self).__init__(api)
        self.row_uuid = row_uuid
        self.external_ids = external_ids

    def run_idl(self, txn):
        try:
            dns = self.api.lookup('DNS', self.row_uuid)
            dns.external_ids = self.external_ids
        except idlutils.RowNotFound:
            msg = "DNS %s does not exist" % self.row_uuid
            raise RuntimeError(msg)


class PgAddCommand(cmd.AddCommand):
    table_name = 'Port_Group'

    def __init__(self, api, name, may_exist=False, **columns):
        super(PgAddCommand, self).__init__(api)
        self.name = name
        self.may_exist = may_exist
        self.columns = columns

    def run_idl(self, txn):
        if self.may_exist:
            try:
                pg = self.api.lookup(self.table_name, self.name)
                self.result = rowview.RowView(pg)
                return
            except idlutils.RowNotFound:
                pass

        pg = txn.insert(self.api._tables[self.table_name])
        pg.name = self.name or ""
        self.set_columns(pg, **self.columns)
        self.result = pg.uuid


class PgDelCommand(cmd.BaseCommand):
    table_name = 'Port_Group'

    def __init__(self, api, name, if_exists=False):
        super(PgDelCommand, self).__init__(api)
        self.name = name
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            pg = self.api.lookup(self.table_name, self.name)
            pg.delete()
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise RuntimeError('Port group %s does not exist' % self.name)


class _PgUpdatePortsHelper(cmd.BaseCommand):
    method = None

    def __init__(self, api, port_group, lsp=None, if_exists=False):
        super(_PgUpdatePortsHelper, self).__init__(api)
        self.port_group = port_group
        self.lsp = [] if lsp is None else self._listify(lsp)
        self.if_exists = if_exists

    def _listify(self, res):
        return res if isinstance(res, (list, tuple)) else [res]

    def _run_method(self, pg, port):
        if not port:
            return

        if isinstance(port, cmd.BaseCommand):
            port = port.result
        elif utils.is_uuid_like(port):
            try:
                port = self.api.lookup('Logical_Switch_Port', port)
            except idlutils.RowNotFound:
                if self.if_exists:
                    return
                raise RuntimeError(
                    'Port %s does not exist' % port)

        getattr(pg, self.method)('ports', port)

    def run_idl(self, txn):
        try:
            pg = self.api.lookup('Port_Group', self.port_group)
        except idlutils.RowNotFound:
            raise RuntimeError('Port group %s does not exist' %
                               self.port_group)

        for lsp in self.lsp:
            self._run_method(pg, lsp)


class PgAddPortCommand(_PgUpdatePortsHelper):
    method = 'addvalue'


class PgDelPortCommand(_PgUpdatePortsHelper):
    method = 'delvalue'


class PgGetCommand(cmd.BaseGetRowCommand):
    table = 'Port_Group'


class GatewayChassisAddCommand(cmd.AddCommand):
    table_name = 'Gateway_Chassis'

    def __init__(self, api, name, chassis_name, priority=0, may_exist=False,
                 **columns):
        super(GatewayChassisAddCommand, self).__init__(api)
        self.name = name
        self.chassis_name = chassis_name
        self.priority = priority
        self.may_exist = may_exist
        self.columns = columns

    def run_idl(self, txn):
        if self.may_exist:
            gwc = self.api.lookup(self.table_name, self.name, None)
        else:
            gwc = None
        if not gwc:
            # If gwc exists with name, this will properly fail if not may_exist
            # since 'name' is indexed
            gwc = txn.insert(self.api.tables[self.table_name])
            gwc.name = self.name
        gwc.priority = self.priority
        self.set_columns(gwc, **self.columns)
        self.result = gwc


class HAChassisGroupAddCommand(cmd.AddCommand):
    table_name = 'HA_Chassis_Group'

    def __init__(self, api, name, may_exist=False, **columns):
        super(HAChassisGroupAddCommand, self).__init__(api)
        self.name = name
        self.may_exist = may_exist
        self.columns = columns

    def run_idl(self, txn):
        if self.may_exist:
            try:
                hcg = self.api.lookup(self.table_name, self.name)
                self.result = rowview.RowView(hcg)
                return
            except idlutils.RowNotFound:
                pass

        hcg = txn.insert(self.api._tables[self.table_name])
        hcg.name = self.name
        self.set_columns(hcg, **self.columns)
        self.result = hcg.uuid


class HAChassisGroupDelCommand(cmd.BaseCommand):
    table_name = 'HA_Chassis_Group'

    def __init__(self, api, name, if_exists=False):
        super(HAChassisGroupDelCommand, self).__init__(api)
        self.name = name
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            hcg = self.api.lookup(self.table_name, self.name)
            hcg.delete()
        except idlutils.RowNotFound:
            if self.if_exists:
                return
            raise RuntimeError(
                'HA Chassis Group %s does not exist' % self.name)


class HAChassisGroupGetCommand(cmd.BaseGetRowCommand):
    table = 'HA_Chassis_Group'


class HAChassisGroupAddChassisCommand(cmd.AddCommand):
    table_name = 'HA_Chassis'

    def __init__(self, api, hcg_id, chassis, priority, **columns):
        super(HAChassisGroupAddChassisCommand, self).__init__(api)
        self.hcg_id = hcg_id
        self.chassis = chassis
        self.priority = priority
        self.columns = columns

    def run_idl(self, txn):
        hc_group = self.api.lookup('HA_Chassis_Group', self.hcg_id)
        found = False
        hc = None
        for hc in hc_group.ha_chassis:
            if hc.chassis_name != self.chassis:
                continue
            found = True
            break
        else:
            hc = txn.insert(self.api.tables[self.table_name])
            hc.chassis_name = self.chassis

        hc.priority = self.priority
        self.set_columns(hc, **self.columns)
        if not found:
            hc_group.addvalue('ha_chassis', hc)

        self.result = hc.uuid


class HAChassisGroupDelChassisCommand(cmd.BaseCommand):
    table_name = 'HA_Chassis'

    def __init__(self, api, hcg_id, chassis, if_exists=False):
        super(HAChassisGroupDelChassisCommand, self).__init__(api)
        self.hcg_id = hcg_id
        self.chassis = chassis
        self.if_exists = if_exists

    def run_idl(self, txn):
        try:
            hc_group = self.api.lookup('HA_Chassis_Group', self.hcg_id)
        except idlutils.RowNotFound:
            if self.if_exists:
                return

        hc = None
        for hc in hc_group.ha_chassis:
            if hc.chassis_name == self.chassis:
                break
        else:
            if self.if_exists:
                return
            raise RuntimeError(
                'HA Chassis %s does not exist' % self.hcg_id)

        hc_group.delvalue('ha_chassis', hc)
        hc.delete()
