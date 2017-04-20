# Copyright 2017 Cloudbase Solutions Srl
#
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


class WaitQueue(object):
    def __init__(self, max_queue_size):
        self.max_queue_size = max_queue_size
        self.init_alert_notification()

    def init_alert_notification(self):
        raise NotImplementedError()

    def alert_notification_consume(self):
        raise NotImplementedError()

    def alert_notify(self):
        raise NotImplementedError()

    @property
    def alert_fileno(self):
        raise NotImplementedError()
