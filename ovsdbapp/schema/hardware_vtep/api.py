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
        physical switch pswitch.

        :param pswitch: The name or uuid of the switch
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
    def unbind_ls(self, pswitch, port, vlan, if_exists=False):
        """Unbind 'switch' from 'pswitch'

        Remove the logical switch binding from the port/vlan combination on
        the  physical switch pswitch.

        :param pswitch:  The name or uuid of the switch
        :type pswitch:   string or uuid.UUID
        :param port:     name of port
        :type port:      string
        :param vlan:     number of VLAN
        :type vlan:      int
        :param switch:   The name or uuid of the switch
        :type switch:    string or uuid.UUID
        :type if_exists: If True, don't fail if the binding doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
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
