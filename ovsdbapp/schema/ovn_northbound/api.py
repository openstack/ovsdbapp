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

import abc

from ovsdbapp import api
from ovsdbapp import constants as const


class API(api.API, metaclass=abc.ABCMeta):
    """An API based off of the ovn-nbctl CLI interface

    This API basically mirrors the ovn-nbctl operations with these changes:
    1. Methods that create objects will return a read-only view of the object
    2. Methods which list objects will return a list of read-only view objects
    """

    @abc.abstractmethod
    def ls_add(self, switch=None, may_exist=False, **columns):
        """Create a logical switch named 'switch'

        :param switch:    The name of the switch (optional)
        :type switch:     string or uuid.UUID
        :param may_exist: If True, don't fail if the switch already exists
        :type may_exist:  boolean
        :param columns:   Additional columns to directly set on the switch
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ls_del(self, switch, if_exists=False):
        """Delete logical switch 'switch' and all its ports

        :param switch:    The name or uuid of the switch
        :type switch:     string or uuid.UUID
        :param if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def ls_list(self):
        """Get all logical switches

        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def ls_get(self, switch):
        """Get logical switch for 'switch'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ls_set_dns_records(self, switch_uuid, dns_uuids):
        """Sets 'dns_records' column on the switch with uuid 'switch_uuid'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ls_clear_dns_records(self, switch):
        """Clears 'dns_records' from the switch with uuid 'switch_uuid'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ls_add_dns_record(self, switch_uuid, dns_uuid):
        """Add the 'dns_record' to the switch's 'dns_records' list

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ls_remove_dns_record(self, switch_uuid, dns_uuid, if_exists=False):
        """Remove the 'dns_record' from the switch's 'dns_records' list

        :param switch_uuid: The uuid of the switch
        :type switch_uuid:  string or uuid.UUID
        :param dns_uuid:    The uuid of the DNS record
        :type dns_uuid:     string or uuid.UUID
        :param if_exists:   If True, don't fail if the DNS record
                            doesn't exist
        :type if_exists:    boolean
        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ls_get_localnet_ports(self, switch, if_exists=False):
        """Get localnet ports on logical switch 'switch'

        :param switch:    The name or uuid of the switch
        :type switch:     string or uuid.UUID
        :param if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists:  boolean
        :returns: :class:`Command` with the RowView list result
        """

    @abc.abstractmethod
    def acl_add(self, switch, direction, priority, match, action, log=False):
        """Add an ACL to 'switch'

        :param switch:    The name or uuid of the switch
        :type switch:     string or uuid.UUID
        :param direction: The traffic direction to match
        :type direction:  'from-lport' or 'to-lport'
        :param priority:  The priority field of the ACL
        :type priority:   int
        :param match:     The match rule
        :type match:      string
        :param action:    The action to take upon match
        :type action:     'allow', 'allow-related', 'allow-stateless', 'drop',
                          or 'reject'
        :param log:       If True, enable packet logging for the ACL
        :type log:        boolean
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def acl_del(self, switch, direction=None, priority=None, match=None,
                if_exists=False):
        """Remove ACLs from 'switch'

        If only switch is supplied, all the ACLs from the logical switch are
        deleted. If direction is also specified, then all the flows in that
        direction will be deleted from the logical switch. If all the fields
        are given, then only flows that match all fields will be deleted.

        :param switch:    The name or uuid of the switch
        :type switch:     string or uuid.UUID
        :param direction: The traffic direction to match
        :type direction:  'from-lport' or 'to-lport'
        :param priority:  The priority field of the ACL
        :type priority:   int
        :param match:     The match rule
        :type match:      string
        :param if_exists: If True, don't fail if the parent logical switch
                          containing the ACL doesn't exist
        :type if_exists:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def acl_list(self, switch):
        """Get the ACLs for 'switch'

        :param switch: The name or uuid of the switch
        :type switch:  string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def pg_acl_add(self, port_group, direction, priority, match, action,
                   log=False, may_exist=False, severity=None, name=None,
                   meter=None, **external_ids):
        """Add an ACL to 'port_group'

        :param port_group:   The name or uuid of the port group
        :type port_group:    string or uuid.UUID
        :param direction:    The traffic direction to match
        :type direction:     'from-lport' or 'to-lport'
        :param priority:     The priority field of the ACL
        :type priority:      int
        :param match:        The match rule
        :type match:         string
        :param action:       The action to take upon match
        :type action:        'allow', 'allow-related', 'allow-stateless',
                             'drop', or 'reject'
        :param log:          If True, enable packet logging for the ACL
        :type log:           boolean
        :param may_exist:    If True, don't fail if the ACL already exists
        :type may_exist:     boolean
        :param severity:     Logging at alert, debug, info, notice or warning
        :type severity:      string
        :param name:         The name of the ACL
        :type name:          string
        :param meter:        The meter name to rate limit logging
        :type meter:         string
        :param external_ids: Values to be added as external_id pairs
        :returns:            :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def pg_acl_del(self, port_group, direction=None, priority=None,
                   match=None, if_exists=False):
        """Remove ACLs from 'port_group'

        If only port_group is supplied, all the ACLs from the logical switch
        are deleted. If direction is also specified, then all the flows in
        that direction will be deleted from the Port Group. If all the fields
        are given, then only flows that match all fields will be deleted.

        :param port_group: The name or uuid of the port group
        :type port_group:  string or uuid.UUID
        :param direction:  The traffic direction to match
        :type direction:   'from-lport' or 'to-lport'
        :param priority:   The priority field of the ACL
        :type priority:    int
        :param match:      The match rule
        :type match:       string
        :param if_exists:  If True, don't fail if the parent port group
                           containing the ACL doesn't exist
        :type if_exists:   boolean
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def pg_acl_list(self, port_group):
        """Get the ACLs for 'port group'

        :param port_group: The name or uuid of the switch
        :type port_group:  string or uuid.UUID
        :returns:          :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def address_set_add(self, name, addresses=None, may_exist=False):
        """Create a set of addresses named 'name'

        :param name:      The name of the address set
        :type name:       string
        :param addresses: One or more IP addresses to add to the address set
        :type addresses:  list of strings
        :param may_exist: If True, don't fail if the address set already exists
        :type may_exist:  boolean
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def address_set_del(self, address_set, if_exists=False):
        """Delete a set of addresses 'address_set'

        :param address_set: The name of the address set
        :type address_set:  string or uuid.UUID
        :param if_exists:   Do not fail if the Address_set row does not exist
        :type if_exists:    bool
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def address_set_get(self, address_set):
        """Get a set of addresses for 'address_set'

        :param address_set: The name of the address set
        :type address_set:  string or uuid.UUID
        :returns:           :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def address_set_list(self):
        """Get all sets of addresses

        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def address_set_add_addresses(self, address_set, addresses):
        """Add a list of addresses to 'address_set'

        :param address_set: The name of the address set
        :type address_set:  string or uuid.UUID
        :param addresses:   One or more IP addresses to add to the address set
        :type addresses:    string or list of strings
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def address_set_remove_addresses(self, address_set, addresses):
        """Remove a list of addresses from 'address_set'

        :param address_set: The name of the address set
        :type address_set:  string or uuid.UUID
        :param addresses:   One or more IP addresses to remove from address set
        :type addresses:    string of list of strings
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def qos_add(self, switch, direction, priority, match, rate=None,
                burst=None, dscp=None, may_exist=False, **columns):
        """Add an Qos rules to 'switch'

        :param switch:    The name or uuid of the switch
        :type switch:     string or uuid.UUID
        :param direction: The traffic direction to match
        :type direction:  'from-lport' or 'to-lport'
        :param priority:  The priority field of the QoS
        :type priority:   int
        :param match:     The match rule
        :type match:      string
        :param dscp:      The dscp mark to take upon match
        :type dscp:       int
        :param rate:      The rate limit to take upon match
        :type rate:       int
        :param burst:     The burst rate limit to take upon match
        :type burst:      int
        :param may_exist: If True, don't fail if the QoS rule already exists
        :type may_exist:  boolean
        :param columns:   Additional columns to directly set on the switch
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def qos_del(self, switch, direction=None, priority=None, match=None,
                if_exists=True):
        """Remove Qos rules from 'switch'

        If only switch is supplied, all the QoS rules from the logical switch
        are deleted. If direction is also specified, then all the flows in
        that direction will be deleted from the logical switch. If all the
        fields are given, then only flows that match all fields will be
        deleted.

        :param switch:    The name or uuid of the switch
        :type switch:     string or uuid.UUID
        :param direction: The traffic direction to match
        :type direction:  'from-lport' or 'to-lport'
        :param priority:  The priority field of the QoS
        :type priority:   int
        :param match:     The match rule
        :type match:      string
        :param if_exists: Do not fail if the Logical_Switch row does not exist
        :type if_exists:  bool
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def qos_list(self, switch):
        """Get the Qos rules for 'switch'

        :param switch: The name or uuid of the switch
        :type switch:  string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def qos_del_ext_ids(self, lswitch_name, external_ids, if_exists=True):
        """Delete all QoS rules related to a floating IP.

        :param lswitch_name: The unique name of the logical switch
        :type lswitch_name: string
        :param external_ids: Pairs of key/value to find in the "external_ids"
        :type external_ids: dict
        :param if_exists: Do not fail if the Logical_Switch row does not exist
        :type if_exists: bool
        :returns: :class:`Command` with no result
        """

    @abc.abstractmethod
    def lsp_add(self, switch, port, parent_name=None, tag=None,
                may_exist=False, **columns):
        """Add logical port 'port' on 'switch'

        NOTE: for the purposes of testing the existence of the 'port',
        'port' is treated as either a name or a uuid, as in ovn-nbctl.

        :param switch:      The name or uuid of the switch
        :type switch:       string or uuid.UUID
        :param port:        The name of the port
        :type port:         string or uuid.UUID
        :param parent_name: The name of the parent port (requires tag)
        :type parent_name:  string
        :param tag:         The tag_request field of the port. 0 causes
                            ovn-northd to assign a unique tag
        :type tag:          int [0, 4095]
        :param may_exist:   If True, don't fail if the switch already exists
        :type may_exist:    boolean
        :param columns:     Additional columns to directly set on the switch
        :returns:           :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lsp_del(self, port, if_exists=False):
        """Delete 'port' from its attached switch

        :param port:     The name or uuid of the port
        :type port:      string or uuid.UUID
        :type if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def lsp_list(self, switch=None):
        """Get the logical ports on switch or all ports if switch is None

        :param switch:  The name or uuid of the switch
        :type switch:   string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lsp_get(self, port):
        """Get logical switch port for 'port'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lsp_get_parent(self, port):
        """Get the parent of 'port' if set

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with port parent string result or
                      "" if not set
        """

    @abc.abstractmethod
    def lsp_set_addresses(self, port, addresses):
        """Set addresses for 'port'

        :param port:      The name or uuid of the port
        :type port:       string or uuid.UUID
        :param addresses: One or more addresses in the format:
                          'unknown', 'router', 'dynamic', or
                          'ethaddr [ipaddr]...'
        :type addresses:  string
        :returns:         :class:`Command` with no result
       """

    @abc.abstractmethod
    def lsp_get_addresses(self, port):
        """Return the list of addresses assigned to port

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    A list of string representations of addresses in the
                     format referenced in lsp_set_addresses
        """

    @abc.abstractmethod
    def lsp_set_port_security(self, port, addresses):
        """Set port security addresses for 'port'

        Sets the port security addresses associated with port to addrs.
        Multiple sets of addresses may be set by using multiple addrs
        arguments. If no addrs argument is given, port will not have
        port security enabled.

        Port security limits the addresses from which a logical port may
        send packets and to which it may receive packets.

        :param port:      The name or uuid of the port
        :type port:       string or uuid.UUID
        :param addresses: The addresses in the format 'ethaddr [ipaddr...]'
                          See `man ovn-nb` and port_security column for details
        :type addresses:  string
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lsp_get_port_security(self, port):
        """Get port security addresses for 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with list of strings described by
                     lsp_set_port_security result
        """

    @abc.abstractmethod
    def lsp_get_up(self, port):
        """Get state of port.

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with boolean result
        """

    @abc.abstractmethod
    def lsp_set_enabled(self, port, is_enabled):
        """Set administrative state of 'port'

        :param port:       The name or uuid of the port
        :type port:        string or uuid.UUID
        :param is_enabled: Whether the port should be enabled
        :type is_enabled:  boolean
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def lsp_get_enabled(self, port):
        """Get administrative state of 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with boolean result
        """

    @abc.abstractmethod
    def lsp_set_type(self, port, port_type):
        """Set the type for 'port

        :param port:      The name or uuid of the port
        :type port:       string or uuid.UUID
        :param port_type: The type of the port
        :type port_type:  string
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lsp_get_type(self, port):
        """Get the type for 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with string result
        """

    @abc.abstractmethod
    def lsp_set_options(self, port, **options):
        """Set options related to the type of 'port'

        :param port:    The name or uuid of the port
        :type port:     string or uuid.UUID
        :param options: keys and values for the port 'options' dict
        :type options:  key: string, value: string
        :returns:       :class:`Command` with no result
        """

    @abc.abstractmethod
    def lsp_get_options(self, port):
        """Get the type-specific options for 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with dict result
        """

    @abc.abstractmethod
    def lsp_set_dhcpv4_options(self, port, dhcp_options_uuid):
        """Set the dhcp4 options for 'port'

        :param port:              The name or uuid of the port
        :type port:               string or uuid.UUID
        :param dhcp_options_uuid: The uuid of the dhcp_options row
        :type dhcp_options_uuid:  uuid.UUID
        :returns:                 :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_add(self, router=None, may_exist=False, **columns):
        """Create a logical router named `router`

        :param router:    The optional name or uuid of the router
        :type router:     string or uuid.UUID
        :param may_exist: If True, don't fail if the router already exists
        :type may_exist:  boolean
        :param **columns: Additional columns to directly set on the router
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lr_del(self, router, if_exists=False):
        """Delete 'router' and all its ports

        :param router: The name or uuid of the router
        :type router:  string or uuid.UUID
        :param if_exists: If True, don't fail if the router doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_list(self):
        """Get the UUIDs of all logical routers

        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lr_get(self, router):
        """Get logical router for 'router'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lrp_add(self, router, port, mac, networks, peer=None, may_exist=False,
                **columns):
        """Add logical port 'port' on 'router'

        :param router:    The name or uuid of the router to attach the port
        :type router:     string or uuid.UUID
        :param mac:       The MAC address of the port
        :type mac:        string
        :param networks:  One or more IP address/netmask to assign to the port
        :type networks:   list of strings
        :param peer:      Optional logical router port connected to this one
        :param may_exist: If True, don't fail if the port already exists
        :type may_exist:  boolean
        :param **columns: Additional column values to directly set on the port
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lrp_del(self, port, router=None, if_exists=None):
        """Delete 'port' from its attached router

        :param port:      The name or uuid of the port
        :type port:       string or uuid.UUID
        :param router:    Only delete router if attached to `router`
        :type router:     string or uuiwhd.UUID
        :param if_exists: If True, don't fail if the port doesn't exist
        :type if_exists:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lrp_list(self, router):
        """Get the UUIDs of all ports on 'router'

        :param router: The name or uuid of the router
        :type router:  string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lrp_get(self, port):
        """Get logical router port for 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lrp_set_enabled(self, port, is_enabled):
        """Set administrative state of 'port'

        :param port:       The name or uuid of the port
        :type port:        string or uuid.UUID
        :param is_enabled: True for enabled, False for disabled
        :type is_enabled:  boolean
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def lrp_get_enabled(self, port):
        """Get administrative state of 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:
        """

    @abc.abstractmethod
    def lrp_set_options(self, port, if_exists=False, **options):
        """Set options related to the type of 'port'

        :param port:      The name or uuid of the port
        :type port:       string or uuid.UUID
        :param options:   keys and values for the port 'options' dict
        :param if_exists: If True, don't fail if the logical router port
                          doesn't exist
        :type if_exists:  boolean
        :type options:    key: string, value: string
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lrp_get_options(self, port):
        """Get the type-specific options for 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with dict result
        """

    @abc.abstractmethod
    def lrp_set_gateway_chassis(self, port, gateway_chassis, priority=0):
        """Set gateway chassis for 'port'

        :param port:            The name or uuid of the port
        :type port:             string or uuid.UUID
        :param gateway_chassis: The name of the gateway chassis
        :type gateway_chassis:  string
        :param priority:        The priority of the gateway chassis
        :type priority:         int
        :returns:               :class:`Command` with no result
        """

    @abc.abstractmethod
    def lrp_get_gateway_chassis(self, port):
        """Get the names of all gateway chassis on 'port'

        :param port: The name or uuid of the port
        :type port:  string or uuid.UUID
        :type port:  int
        :returns:    :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lrp_del_gateway_chassis(self, port, gateway_chassis, if_exists=False):
        """Delete gateway chassis from 'port'

        :param port:            The name or uuid of the port
        :type port:             string or uuid.UUID
        :param gateway_chassis: The name of the gateway chassis
        :type gateway_chassis:  string
        :param if_exists:       If True, don't fail if the gateway chassis
                                doesn't exist
        :type if_exists:        boolean
        :returns:               :class:`Command` with no result
        """

    @abc.abstractmethod
    def lrp_add_networks(self, port, networks, may_exist=False):
        """Add a network to 'port'

        :param port:      The name or uuid of the port
        :type port:       string or uuid.UUID
        :param networks:  One or more IP address/netmask to assign to the port
        :type networks:   list or string
        :param may_exist: If True, don't fail if the networks already exists
        :type may_exist:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lrp_del_networks(self, port, networks, if_exists=False):
        """Remove a network from 'port'

        :param port:      The name or uuid of the port
        :type port:       string or uuid.UUID
        :param networks:  One or more IP address/netmask to remove from
                          the port
        :type networks:   list or string
        :param if_exists: If True, don't fail if the networks doesn't exist
        :type if_exists:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_route_add(self, router, prefix, nexthop, port=None,
                     policy='dst-ip', may_exist=False, ecmp=False,
                     route_table=const.MAIN_ROUTE_TABLE, bfd=None):
        """Add a route to 'router'

        :param router:    The name or uuid of the router
        :type router:     string or uuid.UUID
        :param prefix:    an IPv4/6 prefix for this route, e.g. 192.168.1.0/24
        :type prefix:     type string
        :parm nexthop:    The gateway to use for this route, which should be
                          the IP address of one of `router`'s logical router
                          ports or the IP address of a logical port
        :type nexthop:    string
        :param port:      If specified, packets that match this route will be
                          sent out this port. Otherwise OVN infers the output
                          port based on nexthop.
        :type port:       string
        :param policy:    the policy used to make routing decisions
        :type policy:     string, 'dst-ip' or 'src-ip'
        :param may_exist: If True, don't fail if the route already exists
        :type may_exist:  boolean
        :param ecmp:      Enable ECMP support. If True adding routes with
                          same IP prefix is allowed as long as the nexthop is
                          different
        :type ecmp:       boolean
        :param route_table: The name of route table
        :type route_table:  str
        :param bfd:       A reference to the BFD table row.
        :type bfd:        uuid.UUID
        returns:          :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lr_route_del(self, router, prefix=None, if_exists=False, nexthop=None,
                     route_table=const.MAIN_ROUTE_TABLE):
        """Remove routes from 'router'

        :param router:    The name or uuid of the router
        :type router:     string or uuid.UUID
        :param prefix:    an IPv4/6 prefix to match, e.g. 192.168.1.0/24
        :type prefix:     type string
        :param if_exists: If True, don't fail if the port doesn't exist
        :type if_exists:  boolean
        :parm nexthop:    The gateway to use for this route, which should be
                          the IP address of one of `router`'s logical router
                          ports or the IP address of a logical port
        :type nexthop:    string
        :param route_table: The name of route table
        :type route_table:  str
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_route_list(self, router, route_table=None):
        """Get the UUIDs of static logical routes from 'router'

        :param router:      The name or uuid of the router
        :type router:       string or uuid.UUID
        :param route_table: The name of route table. Pass "" to get routes of
                            global route table only
        :type route_table:  str
        :returns:           :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lr_nat_add(self, router, nat_type, external_ip, logical_ip,
                   logical_port=None, external_mac=None, may_exist=False):
        """Add a NAT to 'router'

        :param router:       The name or uuid of the router
        :type router:        string or uuid.UUID
        :param nat_type:     The type of NAT to be done
        :type nat_type:      NAT_SNAT, NAT_DNAT, or NAT_BOTH
        :param external_ip:  Externally visible Ipv4 address
        :type external_ip:   string
        :param logical_ip:   The logical IPv4 network or address with which
                             `external_ip` is NATted
        :type logical_ip:    string
        :param logical_port: The name of an existing logical switch port where
                             the logical_ip resides
        :type logical_port:  string
        :param external_mac: ARP  replies for the external_ip return the value
                             of `external_mac`. Packets transmitted with
                             source IP address equal to `external_ip` will be
                             sent using `external_mac`.
        :type external_mac:  string
        :param may_exist:    If True, don't fail if the route already exists
                             and if `logical_port` and `external_mac` are
                             specified, they will be updated
        :type may_exist:  boolean
        :returns:      :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lr_nat_del(self, router, nat_type=None, match_ip=None, if_exists=None):
        """Remove NATs from 'router'

        :param router:     The name or uuid of the router
        :type router:      string or uuid.UUID
        :param nat_type:   The type of NAT to match
        :type nat_type:    NAT_SNAT, NAT_DNAT, or NAT_BOTH
        :param match_ip:   The IPv4 address to match on. If
                            `nat_type` is specified and is NAT_SNAT, the IP
                           should be the logical ip, otherwise the IP should
                           be the external IP.
        :type match_ip:    string
        :param if_exists:  If True, don't fail if the port doesn't exist
        :type if_exists:   boolean
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_nat_list(self, router):
        """Get the NATs on 'router'

        :param router: The name or uuid of the router
        :type router:  string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lr_policy_add(self, router, priority, match, action, may_exist=False,
                      **columns):
        """Add a routing policy (PBR) to 'router'

        :param router:    The name or uuid of the router
        :type router:     string or uuid.UUID
        :param priority:  The priority of the policy
        :type priority:   int
        :param match:     The match rule
        :type match:      string
        :param action:    The action to take upon match
        :type action:     POLICY_ACTION_ALLOW, POLICY_ACTION_DROP,
                          POLICY_ACTION_REROUTE
        :param may_exist: If True, don't fail if the policy already exists
                          and instead update the columns
        :type may_exist:  boolean
        :param columns:   Additional columns to directly set on the policy
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lr_policy_del(self, router, priority=None, match=None,
                      if_exists=False):
        """Remove routing policies (PBR) from router

        :param router:     The name or uuid of the router
        :type router:      string or uuid.UUID
        :param priority:   The priority of the rule
        :type priority:    int
        :param match:      The match condition of the rule. This should only
                           be specified if priority is specified.
        :type match:       string
        :param if_exists:  If True, don't fail if the port doesn't exist
        :type if_exists:   boolean
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_policy_list(self, router):
        """Get the list of routing policies on 'router'

        :param router: The name or uuid of the router
        :type router:  string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lb_add(self, lb, vip, ips, protocol=const.PROTO_TCP, may_exist=False,
               **columns):
        """Create a load-balancer or add a VIP to an existing load balancer

        :param lb:        The name or uuid of the load-balancer
        :type lb:         string or uuid.UUID
        :param vip:       A virtual IP in the format IP[:PORT]
        :type vip:        string
        :param ips:       A list of ips in the form IP[:PORT]
        :type ips:        string
        :param protocol:  The IP protocol for load balancing
        :type protocol:   PROTO_TCP or PROTO_UDP
        :param may_exist: If True, don't fail if a LB w/ `vip` exists, and
                          instead, replace the vips on the LB
        :type may_exist:  boolean
        :param columns:   Additional columns to directly set on the load
                          balancer
        :returns:        :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lb_del(self, lb, vip=None, if_exists=False):
        """Remove a load balancer or just the VIP from a load balancer

        If all vips of the load balancer are removed, the load balancer
        instance will be removed as well. If vip parameter is 'None'
        this will cause all vips (and load balancer) to be removed.

        :param lb:        The name or uuid of a load balancer
        :type lb:         string or uuid.UUID
        :param vip:       The VIP to match. If None, match all vips
        :type:            string
        :param if_exists: If True, don't fail if the lb/vip doesn't exist
        :type if_exists:  boolean
        """

    @abc.abstractmethod
    def lb_list(self):
        """Get the UUIDs of all load balanacers"""

    @abc.abstractmethod
    def lb_get(self, lb):
        """Get load balancer for 'lb'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lb_add_health_check(self, lb, vip, **options):
        """Add health check for 'lb'

        :param lb:      The name or uuid of a load balancer
        :type lb:       string or uuid.UUID
        :param vip:     A virtual IP
        :type vip:      string
        :param options: keys and values for the port 'options' dict
        :type options:  key: string, value: string
        :returns:       :class:`Command` with no result
        """

    @abc.abstractmethod
    def lb_del_health_check(self, lb, hc_uuid, if_exists=False):
        """Remove health check from 'lb'

        :param lb:        The name or uuid of a load balancer
        :type lb:         string or uuid.UUID
        :param hc_uuid:   uuid of a health check
        :type hc_uuid:    uuid.UUID
        :param if_exists: If True, don't fail if the hc_uuid doesn't exist
        :type if_exists:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lb_add_ip_port_mapping(self, lb, endpoint_ip, port_name, source_ip):
        """Add IP port mapping to 'lb'

        Maps from endpoint IP to a colon-separated pair of logical port name
        and source IP, e.g. port_name:sourc_ip.

        :param lb:          The name or uuid of a load balancer
        :type lb:           string or uuid.UUID
        :param endpoint_ip: IPv4 address
        :type endpoint_ip:  string
        :param port_name:   The name of a logical port
        :type port_name:    string
        :param source_ip:   IPv4 address
        :type source_ip:    string
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def lb_del_ip_port_mapping(self, lb, endpoint_ip):
        """Remove IP port mapping from 'lb'

        :param lb:          The name or uuid of a load balancer
        :type lb:           string or uuid.UUID
        :param endpoint_ip: IPv4 address
        :type endpoint_ip:  string
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def health_check_set_options(self, hc_uuid, **options):
        """Set options to the 'health_check'

        :param hc_uuid: uuid of the health check
        :type hc_uuid:  uuid.UUID
        :param options: keys and values for the port 'options' dict
        :type options:  key: string, value: string
        :returns:       :class:`Command` with no result
        """

    @abc.abstractmethod
    def health_check_get_options(self, hc_uuid):
        """Get the options for 'health_check'

        :param hc_uuid: uuid of the health check
        :type hc_uuid:  uuid.UUID
        :returns:       :class:`Command` with dict result
        """

    @abc.abstractmethod
    def lr_lb_add(self, router, lb, may_exist=False):
        """Add a load-balancer to 'router'

        :param router:    The name or uuid of the router
        :type router:     string or uuid.UUID
        :param lb:        The name or uuid of the load balancer
        :type lb:         string or uuid.UUID
        :param may_exist: If True, don't fail if lb already assigned to lr
        :type may_exist:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_lb_del(self, router, lb=None, if_exists=False):
        """Remove load-balancers from 'router'

        :param router:   The name or uuid of the router
        :type router:    string or uuid.UUID
        :param lb:       The name or uuid of the load balancer to remove. None
                         to remove all load balancers from the router
        :type lb:        string or uuid.UUID
        :type if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def lr_lb_list(self, router):
        """Get UUIDs of load-balancers on 'router'

        :param router: The name or uuid of the router
        :type router:  string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def ls_lb_add(self, switch, lb, may_exist=False):
        """Add a load-balancer to 'switch'

        :param switch:    The name or uuid of the switch
        :type switch:     string or uuid.UUID
        :param lb:        The name or uuid of the load balancer
        :type lb:         string or uuid.UUID
        :param may_exist: If True, don't fail if lb already assigned to lr
        :type may_exist:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def ls_lb_del(self, switch, lb=None, if_exists=False):
        """Remove load-balancers from 'switch'

        :param switch:   The name or uuid of the switch
        :type switch:    string or uuid.UUID
        :param lb:       The name or uuid of the load balancer to remove. None
                         to remove all load balancers from the switch
        :type lb:        string or uuid.UUID
        :type if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def ls_lb_list(self, switch):
        """Get UUIDs of load-balancers on 'switch'

        :param switch: The name or uuid of the switch
        :type switch:  string or uuid.UUID
        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def dhcp_options_add(self, cidr, **external_ids):
        """Create a DHCP options row with CIDR

        This is equivalent to ovn-nbctl's dhcp-options-create, but renamed
        to be consistent with other object creation methods

        :param cidr:         An IP network in CIDR format
        :type cidr:          string
        :param external_ids: external_id field key/value mapping
        :type external_ids:  key: string, value: string
        :returns:            :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def dhcp_options_del(self, uuid):
        """Delete DHCP options row with 'uuid'

        :param uuid: The uuid of the DHCP Options row to delete
        :type uuid:  string or uuid.UUID
        :returns:    :class:`Command` with no result
        """

    @abc.abstractmethod
    def dhcp_options_list(self):
        """Get all DHCP_Options

        :returns: :class:`Command with RowView list result
        """

    @abc.abstractmethod
    def dhcp_options_get(self, dhcpopt_uuid):
        """Get dhcp options for 'dhcpopt_uuid'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def dhcp_options_set_options(self, uuid, **options):
        """Set the DHCP options for 'uuid'

        :param uuid:  The uuid of the DHCP Options row
        :type uuid:   string or uuid.UUID
        :returns:     :class:`Command` with no result
        """

    @abc.abstractmethod
    def dhcp_options_get_options(self, uuid):
        """Get the DHCP options for 'uuid'

        :param uuid:  The uuid of the DHCP Options row
        :type uuid:   string or uuid.UUID
        :returns:     :class:`Command` with dict result
        """

    @abc.abstractmethod
    def dns_add(self, **columns):
        """Create a DNS row with columns

        :param **columns:  Additional columns to directly set on the dns
        :returns:          :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def dns_del(self, uuid):
        """Delete DNS row with 'uuid'

        :param uuid: The uuid of the DNS row to delete
        :type uuid:  string or uuid.UUID
        :returns:    :class:`Command` with no result
        """

    @abc.abstractmethod
    def dns_get(self, uuid):
        """Get DNS row with 'uuid'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def dns_list(self):
        """Get all DNS rows

        :returns: :class:`Command with RowView list result
        """

    @abc.abstractmethod
    def dns_set_records(self, uuid, **records):
        """Sets the 'records' field of the DNS row

        :param uuid: The uuid of the DNS row to set the records with
        :type uuid:  string or uuid.UUID
        :param records: keys and values for the DNS 'records' dict
        :type records:  key: string, value: string
        :returns:    :class:`Command` with no result
        """

    @abc.abstractmethod
    def dns_add_record(self, uuid, hostname, ips):
        """Add the record 'hostname: ips' into the records column of the DNS

        :param uuid: The uuid of the DNS row to add the record
        :type uuid:  string or uuid.UUID
        :param hostname: hostname as the key to the record dict
        :type ips:  IPs as the value to the hostname key in the 'records'
        :returns:    :class:`Command` with no result
        """

    @abc.abstractmethod
    def dns_remove_record(self, uuid, hostname, if_exists=False):
        """Remove the 'hostname' from the 'records' field of the DNS row

        :param uuid: The uuid of the DNS row to set the records with
        :type uuid:  string or uuid.UUID
        :param hostname: hostname as the key to the record dict
        :param if_exists:   If True, don't fail if the DNS record
                            doesn't exist
        :type if_exists:    boolean
        :returns:    :class:`Command` with no result
        """

    @abc.abstractmethod
    def dns_set_external_ids(self, uuid, **external_ids):
        """Sets the 'external_ids' field of the DNS row

        :param uuid: The uuid of the DNS row to set the external_ids with
        :type uuid:  string or uuid.UUID
        :param external_ids: keys and values for the DNS 'external_ids' dict
        :type external_ids:  key: string, value: string
        :returns:    :class:`Command` with no result
        """

    @abc.abstractmethod
    def dns_set_options(self, uuid, if_exists=True, **options):
        """Sets the 'options' field of the DNS row

        :param uuid: The uuid of the DNS row to set the options with
        :type uuid:  string or uuid.UUID
        :param if_exists: Do not fail if the DNS row does not exist
        :type if_exists:  boolean
        :param options: keys and values for the DNS 'options' dict
        :type options:  key: string, value: string
        :returns:    :class:`Command` with no result
        """

    @abc.abstractmethod
    def pg_add(self, name=None, may_exist=False, **columns):
        """Create a port group

        :param name:        The name of the port group (optional)
        :type name:         string
        :param may_exist:   If True, don't fail if the port group already
                            exists
        :type may_exist:    bool
        :param columns:     Additional columns to directly set on the port
                            group (e.g external_ids, ports, acls)
        :type columns:      dictionary
        :returns:           :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def pg_del(self, name, if_exists=False):
        """Delete a port group

        :param name:        The name of the port group
        :type name:         string
        :param if_exists:   If True, don't fail if the port group
                            doesn't exist
        :type if_exists:    boolean
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def pg_add_ports(self, pg_id, lsp):
        """Add a list of logical port to a port group

        :param pg_id:     The name or uuid of the port group
        :type pg_id:      string or uuid.UUID
        :param lsp:       A list of :class:`Command` Logical_Switch_Port
                          instance result or UUID
        :type lsp:        A list of :class:`Command` Logical_Switch_Port
                          or string or uuid.UUID
        A Logical_Switch_Port instance or string
                          or uuid.UUID
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def pg_del_ports(self, pg_id, lsp, if_exists=False):
        """Delete a list of logical port from a port group

        :param pg_id:     The name or uuid of the port group
        :type pg_id:      string or uuid.UUID
        :param lsp:       A list of :class:`Command` Logical_Switch_Port
                          instance result or UUID
        :type lsp:        A list of :class:`Command` Logical_Switch_Port
                          or string or uuid.UUID
        :type if_exists:  If True, don't fail if the logical port(s) doesn't
                          exist
        :type if_exists:  boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def pg_get(self, pg_id):
        """Get port group

        :param pg_id: The name or uuid of the port group
        :type pg_id:  string or uuid.UUID
        :returns:     :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ha_chassis_group_add(self, name, may_exist=False, **columns):
        """Create a HA Chassis Group

        :param name:        The name of the ha chassis group
        :type name:         string
        :param may_exist:   If True, don't fail if the ha chassis group
                            already exists
        :type may_exist:    bool
        :param columns:     Additional columns to directly set on the ha
                            chassis group (e.g external_ids)
        :type columns:      dictionary
        :returns:           :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ha_chassis_group_del(self, name, if_exists=False):
        """Delete a HA Chassis Group

        :param name:        The name of the ha chassis group
        :type name:         string
        :param if_exists:   If True, don't fail if the ha chassis group
                            doesn't exist
        :type if_exists:    boolean
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def ha_chassis_group_get(self, name):
        """Get HA Chassis Group

        :param name: The name or uuid of the ha chassis group
        :type name:  string or uuid.UUID
        :returns:    :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ha_chassis_group_add_chassis(self, hcg_id, chassis, priority,
                                     **columns):
        """Add a HA Chassis to a HA Chassis Group

        :param hcg_id:   The name or uuid of the ha chassis group
        :type hcg_id:    string or uuid.UUID
        :param chassis:  The name of the ha chassis
        :type chassis:   string
        :param priority: The priority of the ha chassis
        :type priority:  int
        :param columns:  Additional columns to directly set on the ha
                         chassis (e.g external_ids)
        :type columns:   dictionary
        :returns:        :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ha_chassis_group_del_chassis(self, hcg_id, chassis, if_exists=False):
        """Delete a HA Chassis from a HA Chassis Group

        :param hcg_id:     The name or uuid of the ha chassis group
        :type hcg_id:      string or uuid.UUID
        :param chassis:    The name of the ha chassis
        :type chassis:     string
        :param if_exists:  If True, don't fail if the ha chassis
                           doesn't exist
        :type if_exists:   boolean
        :returns:          :class:`Command` with no result
        """

    @abc.abstractmethod
    def meter_add(self, name, unit, rate=1, fair=False, burst_size=0,
                  action=None, may_exist=False, **columns):
        """Create a Meter

        :param name:        The name of the meter
        :type name:         string
        :param unit:        The unit of the meter (e.g kbps, pktps)
        :type unit:         string
        :param rate:        The rate of the meter
        :type rate:         int
        :param fair:        Specify whether meter rate limits its references
                            individually (True) or as a shared pool (False)
        :type fair:         boolean
        :param burst_size:  The maximum burst allowed for the band in kilobits
                            or packets, depending on the unit used
        :type burst_size:   int
        :param action:      The action of the meter. 'None' can be used as an
                            alternate for the 'drop' value.
        :type action:       string
        :param may_exist:   If True, don't fail if the meter already exists
        :type may_exist:    boolean
        :param columns:     Additional columns to directly set on the meter
                            (e.g external_ids)
        :type columns:      dictionary
        :returns:           :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def meter_del(self, meter, if_exists=False):
        """Delete a meter

        :param meter:       The name or uuid of the meter
        :type meter:        string or uuid.UUID
        :param if_exists:   If True, don't fail if the meter doesn't exist
        :type if_exists:    boolean
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def meter_list(self):
        """Get all meters

        :returns:      :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def meter_get(self, meter):
        """Get the meter

        :param meter:  The name or uuid of the meter
        :type meter:   string or uuid.UUID
        :returns:      :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def bfd_add(self, logical_port, dst_ip, min_tx=None, min_rx=None,
                detect_mult=None, external_ids=None, options=None,
                may_exist=False):
        """Create a BFD entry

        :param logical_port: Name of logical port where BFD engine should run.
        :type logical_port:  str
        :param dst_ip:       BFD peer IP address.
        :type dst_ip:        str
        :param min_tx:       This is the minimum interval, in milliseconds,
                             that the local system would like to use when
                             transmitting BFD Control packets, less any jitter
                             applied. The value zero is reserved. Default value
                             is 1000 ms.
        :type min_tx:        Optional[int]
        :param min_rx:       This is the minimum interval, in milliseconds,
                             between received BFD Control packets that this
                             system is capable of supporting, less any jitter
                             applied by the sender. If this value is zero, the
                             transmitting system does not want the remote
                             system to send any periodic BFD Control packets.
        :type min_rx:        Optional[int]
        :param detect_mult:  Detection time multiplier.  The negotiated
                             transmit interval, multiplied by this value,
                             provides the Detection Time for the receiving
                             system in Asynchronous mode. Default value is 5.
        :type detect_mult:   Optional[int]
        :param external_ids: Values to be added as external_id pairs.
        :type external_ids:  Optional[Dict[str,str]]
        :param options:      Reserved for future use.
        :type options:       Optional[Dict[str,str]]
        :param may_exist:    If True, update any existing BFD entry if it
                             already exists.  Default is False which will raise
                             an error if a BFD entry with same logical_port,
                             dst_ip pair already exists.
        :type may_exist:     Optional[bool]
        :returns:            :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def bfd_del(self, uuid):
        """Delete a BFD entry

        :param uuid:   The uuid of the BFD entry
        :type uuid:    uuid.UUID
        :returns:      :class:`Command` with no result
        """

    @abc.abstractmethod
    def bfd_find(self, logical_port, dst_ip):
        """Find BFD entry.

        :param logical_port: Name of logical port where BFD engine runs.
        :type logical_port:  str
        :param dst_ip:       BFD peer IP address.
        :type dst_ip:        str
        :returns:            :class:`Command` with List[Dict[str,any]] result
        """

    @abc.abstractmethod
    def bfd_get(self, uuid):
        """Get the BFD entry

        :param uuid:   The uuid of the BFD entry
        :type uuid:    uuid.UUID
        :returns:      :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def mirror_get(self, uuid):
        """Get the Mirror entry"""

    @abc.abstractmethod
    def mirror_del(self, mirror):
        """Delete a Mirror"""

    @abc.abstractmethod
    def mirror_add(self, name, mirror_type, index, direction_filter, dest,
                   external_ids=None,
                   may_exist=False):
        """Create a Mirror entry

        :param name:    Name of the Mirror to create.
        :type name:     str
        :param mirror_type:    The type of the mirroring can be gre or erspan.
        :type mirror_type:     str
        :param index:   The index filed will be used for the Index field in
                        ERSPAN header as decimal, and as hexadecimal value in
                        the SpanID field, for GRE mirrors it will be the Key
                        field.
        :type index:    int
        :param direction_filter:  The direction of the traffic to be mirrored,
                                  can be from-lport and to-lprt.
        :type direction_filter:   str
        :param dest:    The destination IP address of the mirroring.
        :type dest:     str


        :param external_ids: Values to be added as external_id pairs.
        :type external_ids:  Optional[Dict[str,str]]
        :param may_exist:    If True, update any existing Mirror entry if it
                             already exists.  Default is False which will raise
                             an error if a Mirror entry with same logical_port,
                             sink pair already exists.
        :type may_exist:     Optional[bool]
        :returns:            :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lsp_attach_mirror(self, port, mirror, may_exist=False):
        """Attaches an lsp to the given mirror

        :param port: the id of the lsp
        :type port: str
        :param mirror: the name or ID of the mirror.
        :type mirror: str
        :param may_exist:    If True, don't fail if the mirror_rule already
                             exists.
        :type may_exist:     Optional[bool]
        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def lsp_detach_mirror(self, port, mirror, if_exist=False):
        """Detaches an lsp from the given mirror

        :param port: the id of the lsp
        :type port: str
        :param mirror: the name or ID of the mirror
        :type mirror: str
        :param if_exist:    If True, don't fail if the mirror_rules entry
                            doesn't exist.
        :type if_exist:     Optional[bool]
        :returns: :class:`Command` with RowView result
        """
