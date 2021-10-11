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
    """An API based off of the ovn-ic-nbctl CLI interface

    This API basically mirrors the ovn-ic-nbctl operations with these changes:
    1. Methods that create objects will return a read-only view of the object
    2. Methods which list objects will return a list of read-only view objects
    """

    @abc.abstractmethod
    def ts_add(self, switch, may_exist=False, **columns):
        """Create a transit switch named 'switch'

        :param switch:    The name of the switch
        :type switch:     string or uuid.UUID
        :param may_exist: If True, don't fail if the switch already exists
        :type may_exist:  boolean
        :param columns:   Additional columns to directly set on the switch
        :returns:         :class:`Command` with RowView result
        """

    @abc.abstractmethod
    def ts_del(self, switch, if_exists=False):
        """Delete transit switch 'switch' and all its ports

        :param switch:   The name or uuid of the switch
        :type switch:    string or uuid.UUID
        :param if_exists: If True, don't fail if the switch doesn't exist
        :type if_exists: boolean
        :returns:        :class:`Command` with no result
        """

    @abc.abstractmethod
    def ts_list(self):
        """Get all transit switches

        :returns: :class:`Command` with RowView list result
        """

    @abc.abstractmethod
    def ts_get(self, switch):
        """Get transit switch for 'switch'

        :returns: :class:`Command` with RowView result
        """
