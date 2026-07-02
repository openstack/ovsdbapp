# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os

from ovs.stream import Stream
import testscenarios

from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp import constants
from ovsdbapp.schema.open_vswitch import impl_idl as ovs_impl
from ovsdbapp.schema.ovn_northbound import impl_idl as nb_impl
from ovsdbapp.schema.ovn_southbound import impl_idl as sb_impl
from ovsdbapp.tests import base
from ovsdbapp.tests.functional.schema import fixtures
from ovsdbapp import venv


class NbApiFixture(fixtures.ApiImplFixture):
    api_cls = nb_impl.OvnNbApiIdlImpl


class SbApiFixture(fixtures.ApiImplFixture):
    api_cls = sb_impl.OvnSbApiIdlImpl


class OvsApiFixture(fixtures.ApiImplFixture):
    api_cls = ovs_impl.OvsdbIdl


class TestVenvConnections(testscenarios.TestWithScenarios, base.TestCase):
    scenarios = [
        ('unix', dict(protocol=None)),
        ('tcp', dict(protocol='tcp')),
        ('ssl', dict(protocol='ssl')),
    ]

    def setUp(self):
        super().setUp()
        virtualenv = self.useFixture(venv.VenvFixture(remove=True))
        self.ovsvenv = self.useFixture(venv.OvsOvnVenvFixture(
            virtualenv,
            ovsdir=os.getenv('OVS_SRCDIR'),
            ovndir=os.getenv('OVN_SRCDIR'),
            protocol=self.protocol))
        if self.protocol == 'ssl':
            Stream.ssl_set_private_key_file(
                self.ovsvenv.ssl_config.private_key)
            Stream.ssl_set_certificate_file(
                self.ovsvenv.ssl_config.certificate)
            Stream.ssl_set_ca_cert_file(
                self.ovsvenv.ssl_config.ca_cert)

    def _create_api(self, fixture_cls, conn_str, schema):
        idl = connection.OvsdbIdl.from_server(conn_str, schema)
        conn = connection.Connection(idl, constants.DEFAULT_TIMEOUT)
        self.addCleanup(conn.stop)
        return self.useFixture(fixture_cls(conn)).obj

    def test_ovn_northbound(self):
        conn_str = self.ovsvenv.ovnnb_connection
        api = self._create_api(NbApiFixture, conn_str, 'OVN_Northbound')
        nb_global = api.db_list('NB_Global').execute(check_error=True)
        self.assertEqual(1, len(nb_global))

    def test_ovn_southbound(self):
        conn_str = self.ovsvenv.ovnsb_connection
        api = self._create_api(SbApiFixture, conn_str, 'OVN_Southbound')
        sb_global = api.db_list('SB_Global').execute(check_error=True)
        self.assertEqual(1, len(sb_global))

    def test_open_vswitch(self):
        conn_str = self.ovsvenv.ovs_connection
        api = self._create_api(OvsApiFixture, conn_str, 'Open_vSwitch')
        ovs = api.db_list('Open_vSwitch').execute(check_error=True)
        self.assertEqual(1, len(ovs))
