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

from ovsdbapp.schema.ovn_northbound import impl_idl
from ovsdbapp.tests.functional.schema import fixtures


class LogicalSwitchFixture(fixtures.ImplIdlFixture):
    api = impl_idl.OvnNbApiIdlImpl
    create = 'ls_add'
    delete = 'ls_del'


class DhcpOptionsFixture(fixtures.ImplIdlFixture):
    api = impl_idl.OvnNbApiIdlImpl
    create = 'dhcp_options_add'
    delete = 'dhcp_options_del'
    delete_args = {}


class LogicalRouterFixture(fixtures.ImplIdlFixture):
    api = impl_idl.OvnNbApiIdlImpl
    create = 'lr_add'
    delete = 'lr_del'


class LoadBalancerFixture(fixtures.ImplIdlFixture):
    api = impl_idl.OvnNbApiIdlImpl
    create = 'lb_add'
    delete = 'lb_del'


class DnsFixture(fixtures.ImplIdlFixture):
    api = impl_idl.OvnNbApiIdlImpl
    create = 'dns_add'
    delete = 'dns_del'
    delete_args = {}


class PortGroupFixture(fixtures.ImplIdlFixture):
    api = impl_idl.OvnNbApiIdlImpl
    create = 'pg_add'
    delete = 'pg_del'
