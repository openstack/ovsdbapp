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


class ImplIdlFixture(fixtures.Fixture):
    create, delete = (None, None)
    delete_args = {'if_exists': True}
    delete_id = 'uuid'

    def __init__(self, api, *args, **kwargs):
        super(ImplIdlFixture, self).__init__()
        self.api = api
        self.args = args
        self.kwargs = kwargs

    def _setUp(self):
        create_fn = getattr(self.api, self.create)
        delete_fn = getattr(self.api, self.delete)
        self.obj = create_fn(*self.args, **self.kwargs).execute(
            check_error=True)
        del_value = getattr(self.obj, self.delete_id)
        self.addCleanup(delete_fn(del_value,
                        **self.delete_args).execute, check_error=True)
