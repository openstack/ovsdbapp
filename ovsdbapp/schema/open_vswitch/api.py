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

import abc

from ovsdbapp import api


class API(api.API, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_manager(self, connection_uri):
        """Create a command to add a Manager to the OVS switch

        This API will add a new manager without overriding the existing ones.

        :param connection_uri: target to which manager needs to be set
        :type connection_uri: string, see ovs-vsctl manpage for format
        :returns:           :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def get_manager(self):
        """Create a command to get Manager list from the OVS switch

        :returns: :class:`Command` with list of Manager names result
        """

    @abc.abstractmethod
    def remove_manager(self, connection_uri):
        """Create a command to remove a Manager from the OVS switch

        This API will remove the manager configured on the OVS switch.

        :param connection_uri: target identifying the manager uri that
                               needs to be removed.
        :type connection_uri: string, see ovs-vsctl manpage for format
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def add_br(self, name, may_exist=True, datapath_type=None):
        """Create a command to add an OVS bridge

        :param name:            The name of the bridge
        :type name:             string
        :param may_exist:       Do not fail if bridge already exists
        :type may_exist:        bool
        :param datapath_type:   The datapath_type of the bridge
        :type datapath_type:    string
        :returns:               :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def del_br(self, name, if_exists=True):
        """Create a command to delete an OVS bridge

        :param name:      The name of the bridge
        :type name:       string
        :param if_exists: Do not fail if the bridge does not exist
        :type if_exists:  bool
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def br_exists(self, name):
        """Create a command to check if an OVS bridge exists

        :param name: The name of the bridge
        :type name:  string
        :returns:    :class:`Command` with bool result
        """

    @abc.abstractmethod
    def port_to_br(self, name):
        """Create a command to return the name of the bridge with the port

        :param name: The name of the OVS port
        :type name:  string
        :returns:    :class:`Command` with bridge name result
        """

    @abc.abstractmethod
    def iface_to_br(self, name):
        """Create a command to return the name of the bridge with the interface

        :param name: The name of the OVS interface
        :type name:  string
        :returns:    :class:`Command` with bridge name result
        """

    @abc.abstractmethod
    def list_br(self):
        """Create a command to return the current list of OVS bridge names

        :returns: :class:`Command` with list of bridge names result
        """

    @abc.abstractmethod
    def br_get_external_id(self, name, field):
        """Create a command to return a field from the Bridge's external_ids

        :param name:  The name of the OVS Bridge
        :type name:   string
        :param field: The external_ids field to return
        :type field:  string
        :returns:     :class:`Command` with field value result
        """

    @abc.abstractmethod
    def br_set_external_id(self, name, field, value):
        """Create a command to set the OVS Bridge's external_ids

        :param name:  The name of the bridge
        :type name:   string
        :param field: The external_ids field to set
        :type field:  string
        :param value: The external_ids value to set
        :type value:  string
        :returns:     :class:`Command` with field no result
        """

    @abc.abstractmethod
    def set_controller(self, bridge, controllers):
        """Create a command to set an OVS bridge's OpenFlow controllers

        :param bridge:      The name of the bridge
        :type bridge:       string
        :param controllers: The controller strings
        :type controllers:  list of strings, see ovs-vsctl manpage for format
        :returns:           :class:`Command` with no result
        """

    @abc.abstractmethod
    def del_controller(self, bridge):
        """Create a command to clear an OVS bridge's OpenFlow controllers

        :param bridge: The name of the bridge
        :type bridge:  string
        :returns:      :class:`Command` with no result
        """

    @abc.abstractmethod
    def get_controller(self, bridge):
        """Create a command to return an OVS bridge's OpenFlow controllers

        :param bridge: The name of the bridge
        :type bridge:  string
        :returns:      :class:`Command` with list of controller strings result
        """

    @abc.abstractmethod
    def set_fail_mode(self, bridge, mode):
        """Create a command to set an OVS bridge's failure mode

        :param bridge: The name of the bridge
        :type bridge:  string
        :param mode:   The failure mode
        :type mode:    "secure" or "standalone"
        :returns:      :class:`Command` with no result
        """

    @abc.abstractmethod
    def add_port(self, bridge, port, may_exist=True, **interface_attrs):
        """Create a command to add a port to an OVS bridge

        :param bridge:    The name of the bridge
        :type bridge:     string
        :param port:      The name of the port
        :type port:       string
        :param may_exist: Do not fail if the port already exists
        :type may_exist:  bool
        :param interface_attrs: Additional parameters to be set in the
                          "Interface" register (not the "Port" register) that
                          is created along with the "Port" one
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def del_port(self, port, bridge=None, if_exists=True):
        """Create a command to delete a port an OVS port

        :param port:      The name of the port
        :type port:       string
        :param bridge:    Only delete port if it is attached to this bridge
        :type bridge:     string
        :param if_exists: Do not fail if the port does not exist
        :type if_exists:  bool
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def list_ports(self, bridge):
        """Create a command to list the names of ports on a bridge

        :param bridge: The name of the bridge
        :type bridge:  string
        :returns:      :class:`Command` with list of port names result
        """

    @abc.abstractmethod
    def list_ifaces(self, bridge):
        """Create a command to list the names of interfaces on a bridge

        :param bridge: The name of the bridge
        :type bridge:  string
        :returns:      :class:`Command` with list of interfaces names result
        """

    @abc.abstractmethod
    def iface_get_external_id(self, name, field):
        """Create a command to return a field from the Interface's external_ids

        :param name:  The name of the interface
        :type name:   string
        :param field: The external_ids field to return
        :type field:  string
        :returns:     :class:`Command` with field value result
        """

    @abc.abstractmethod
    def iface_set_external_id(self, name, field, value):
        """Create a command to set the OVS Interface's external_ids

        :param name:  The name of the interface
        :type name:   string
        :param field: The external_ids field to set
        :type field:  string
        :param value: The external_ids value to set
        :type value:  string
        :returns:     :class:`Command` with field no result
        """
