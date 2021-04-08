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

import threading
import time
import uuid

from ovsdbapp.backend.ovs_idl import event
from ovsdbapp.backend.ovs_idl import idlutils
from ovsdbapp.schema.ovn_northbound import impl_idl
from ovsdbapp.tests.functional import base


class TestOvnNbIndex(base.FunctionalTestCase):
    schemas = ['OVN_Northbound']

    def setUp(self):
        super(TestOvnNbIndex, self).setUp()
        self.api = impl_idl.OvnNbApiIdlImpl(self.connection)
        self.lsp_name = str(uuid.uuid4())
        self.a = None

    def _create_ls(self):
        time.sleep(1)  # Wait a bit to allow a first unsuccessful lookup().
        self.api.db_create('Logical_Switch', name=self.lsp_name).execute()

    def test_lookup_with_timeout_and_notify_handler(self):
        notify_handler = event.RowEventHandler()
        self.api.idl.notify = notify_handler.notify
        t_create = threading.Thread(target=self._create_ls, args=())
        t_create.start()
        ret = self.api.lookup('Logical_Switch', self.lsp_name, timeout=3,
                              notify_handler=notify_handler)
        self.assertEqual(self.lsp_name, ret.name)
        t_create.join()

    def _test_lookup_exception(self, timeout, notify_handler):
        if notify_handler:
            self.api.idl.notify = notify_handler.notify
        t_create = threading.Thread(target=self._create_ls, args=())
        t_create.start()
        self.assertRaises(idlutils.RowNotFound, self.api.lookup,
                          'Logical_Switch', self.lsp_name, timeout=timeout,
                          notify_handler=notify_handler)
        t_create.join()

    def test_lookup_without_timeout(self):
        self._test_lookup_exception(0, event.RowEventHandler())

    def test_lookup_without_event_handler(self):
        self._test_lookup_exception(3, None)

    def test_lookup_without_timeout_and_event_handler(self):
        self._test_lookup_exception(0, None)
