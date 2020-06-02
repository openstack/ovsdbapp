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

import fixtures

from ovsdbapp.backend.ovs_idl import vlog


class OvsdbVlogFixture(fixtures.Fixture):
    def __init__(self, *args, **kwargs):
        """Constructor for the OvsdbVlogVixture

        The OvsdbVlogFixture will call vlog.use_python_logger with any args or
        kwargs passed and call vlog.reset_logger() on cleanup
        """
        self.args = args
        self.kwargs = kwargs

    def _setUp(self):
        vlog.use_python_logger(*self.args, **self.kwargs)
        self.addCleanup(vlog.reset_logger)
