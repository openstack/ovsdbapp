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
    """An API based off of the vtep-ctl CLI interface

    This API basically mirrors the vtep-ctl operations with these changes:
    1. Methods that create objects will return a read-only view of the object
    2. Methods which list objects will return a list of read-only view objects
    """

    @abc.abstractmethod
    def add_ps(self, pswitch, may_exist=False, **columns):
        """Create a physical switch named 'pswitch'

        :param pswitch:   The name of the switch
        :type pswitch:    string or uuid.UUID
        :param may_exist: If True, don't fail if the switch already exists
        :type may_exist:  boolean
        :param columns:   Additional columns to directly set on the switch
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def del_ps(self, pswitch, if_exists=False):
        """Delete physical switch 'pswitch' and all its ports

        :param pswitch:  The name or uuid of the switch
        :type pswitch:   string or uuid.UUID
        :type if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def list_ps(self):
        """Get all physical switches

        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def get_ps(self, pswitch):
        """Get physical switch for 'pswitch'

        :param pswitch: The name of the pswitch
        :type pswitch:  string or uuid.UUID
        :returns:       :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def add_port(self, pswitch, port, may_exist=False):
        """Add a port named 'port' to physical switch named 'pswitch'

        :param pswitch:   The name of the switch
        :type pswitch:    string or uuid.UUID
        :param port:      The name of the port
        :type port:       string or uuid.UUID
        :param may_exist: If True, don't fail if the port already exists in
                          physical switch
        :type may_exist:  boolean
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def del_port(self, pswitch, port, if_exists=False):
        """Delete a port named 'port' from physical switch named 'pswitch'

        :param pswitch:  The name or uuid of the switch
        :type pswitch:   string or uuid.UUID
        :param port:     The name of the port
        :type port:      string or uuid.UUID
        :type if_exists: If True, don't fail if the switch doesn't exist in
                         physical switch
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def list_ports(self, pswitch):
        """Get all ports of physical switch 'pswitch'

        :param pswitch: The name of the pswitch
        :type pswitch:  string or uuid.UUID
        :returns:       :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def get_port(self, port):
        """Get physical port for 'port'

        :param port: The name of the port
        :type port:  string or uuid.UUID
        :returns:    :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def add_ls(self, switch, may_exist=False, **columns):
        """Create a logical switch named 'switch'

        :param switch:    The name of the switch
        :type switch:     string or uuid.UUID
        :param may_exist: If True, don't fail if the switch already exists
        :type may_exist:  boolean
        :param columns:   Additional columns to directly set on the switch
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def del_ls(self, switch, if_exists=False):
        """Delete logical switch 'switch' and all its ports

        :param switch:   The name or uuid of the switch
        :type switch:    string or uuid.UUID
        :type if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def list_ls(self):
        """Get all logical switches

        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def get_ls(self, switch):
        """Get logical switch for 'switch'

        :returns: :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def bind_ls(self, pswitch, port, vlan, switch):
        """Bind 'switch' to 'pswitch'

        Bind logical switch to the port/vlan combination on the
        physical switch 'pswitch'.

        :param pswitch: The name or uuid of the physical switch
        :type pswitch:  string or uuid.UUID
        :param port:    name of port
        :type port:     string
        :param vlan:    number of VLAN
        :type vlan:     int
        :param switch:  The name or uuid of the switch
        :type switch:   string or uuid.UUID
        :returns:       :class:`Command` with no result
        """

    @abc.abstractmethod
    def unbind_ls(self, pswitch, port, vlan):
        """Unbind 'switch' from 'pswitch'

        Remove the logical switch binding from the port/vlan combination on
        the  physical switch pswitch.

        :param pswitch:  The name or uuid of the physical switch
        :type pswitch:   string or uuid.UUID
        :param port:     name of port
        :type port:      string
        :param vlan:     number of VLAN
        :type vlan:      int
        :param switch:   The name or uuid of the switch
        :type switch:    string or uuid.UUID
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def list_local_macs(self, switch):
        """Get all local MACs for 'switch'

        :param switch: The name of the switch
        :type switch:  string or uuid.UUID
        :returns:      :class:`Command` with list of RowView lists result.
                       First list contains 'Ucast_Macs_Local' table records,
                       second list contains 'Mcast_Macs_Local' table records.
        """

    @abc.abstractmethod
    def list_remote_macs(self, switch):
        """Get all remote MACs for 'switch'

        :param switch: The name of the switch
        :type switch:  string or uuid.UUID
        :returns:      :class:`Command` with list of RowView lists result.
                       First list contains 'Ucast_Macs_Remote' table records,
                       second list contains 'Mcast_Macs_Remote' table records.
        """

    @abc.abstractmethod
    def clear_local_macs(self, switch):
        """Clear the local MAC bindings for 'switch'

        :param switch: The name of the switch
        :type switch:  string or uuid.UUID
        :returns:      :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def clear_remote_macs(self, switch):
        """Clear the remote MAC bindings for 'switch'

        :param switch: The name of the switch
        :type switch:  string or uuid.UUID
        :returns:      :class:`Command` with RowView result
        """
