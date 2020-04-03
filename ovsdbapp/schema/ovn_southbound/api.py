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
    """An API based off of the ovn-sbctl CLI interface

    This API basically mirrors the ovn-nbctl operations with these changes:
    1. Methods that create objects will return a read-only view of the object
    2. Methods which list objects will return a list of read-only view objects
    """

    @abc.abstractmethod
    def chassis_add(self, chassis, encap_types, encap_ip, may_exist=False,
                    **columns):
        """Creates  a  new chassis

        :param chassis:     The name of the chassis to create
        :type chassis:      string
        :param encap_types: Tunnel types for the chassis
        :type encap_types:  list of strings
        :encap_ip:          The destination IP for each tunnel
        :type encap_ip:     string
        :param may_exist:   Don't fail if chassis named `chassis` exists
        :type may_exist:    boolean
        :param columns:     Additional column values to set
        :type columns:      key/value pairs
        :returns:           :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def chassis_del(self, chassis, if_exists=False):
        """Deletes chassis and its encaps and gateway_ports

        :param chassis:   The name of the chassis to delete
        :type chassis:    string
        :param if_exsits: Don't fail if `chassis` doesn't exist
        :param if_exists: boolean
        :returns:         :class:`Command` with no result
        """

    @abc.abstractmethod
    def chassis_list(self):
        """Retrieve all chassis

        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def lsp_bind(self, port, chassis, may_exist=False):
        """Bind a logical port to a chassis

        :param port:      The name of the logical port to bind
        :type port:       string
        :param chassis:   The name of the chassis
        :type chassis:    string
        :param may_exist: Don't fail if port is already bound to a chassis
        :type may_exist:  boolean
        """

    @abc.abstractmethod
    def lsp_unbind(self, port, if_exists=False):
        """Unbind a logical port from its chassis

        :param port:      The name of the port to unbind
        :type port:       string
        :param if_exists: Don't fail if the port binding doesn't exist
        :type if_exists:  boolean
        """
