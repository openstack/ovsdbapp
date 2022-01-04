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


class NbApiFixture(fixtures.ApiImplFixture):
    api_cls = impl_idl.OvnNbApiIdlImpl


class LogicalSwitchFixture(fixtures.ImplIdlFixture):
    create = 'ls_add'
    delete = 'ls_del'


class DhcpOptionsFixture(fixtures.ImplIdlFixture):
    create = 'dhcp_options_add'
    delete = 'dhcp_options_del'
    delete_args = {}


class LogicalRouterFixture(fixtures.ImplIdlFixture):
    create = 'lr_add'
    delete = 'lr_del'


class LoadBalancerFixture(fixtures.ImplIdlFixture):
    create = 'lb_add'
    delete = 'lb_del'


class DnsFixture(fixtures.ImplIdlFixture):
    create = 'dns_add'
    delete = 'dns_del'
    delete_args = {}


class PortGroupFixture(fixtures.ImplIdlFixture):
    create = 'pg_add'
    delete = 'pg_del'


class AddressSetFixture(fixtures.ImplIdlFixture):
    create = 'address_set_add'
    delete = 'address_set_del'


class MeterFixture(fixtures.ImplIdlFixture):
    create = 'meter_add'
    delete = 'meter_del'
