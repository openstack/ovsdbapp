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
import atexit
import logging
import threading

import six
from six.moves import queue as Queue

LOG = logging.getLogger(__name__)
STOP_EVENT = ("STOP", None, None, None)


@six.add_metaclass(abc.ABCMeta)
class RowEvent(object):
    ROW_CREATE = "create"
    ROW_UPDATE = "update"
    ROW_DELETE = "delete"
    ONETIME = False
    event_name = 'RowEvent'

    def __init__(self, events, table, conditions, old_conditions=None):
        self.table = table
        self.events = events
        self.conditions = conditions
        self.old_conditions = old_conditions

    @property
    def key(self):
        return (self.__class__, self.table, tuple(self.events))

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        try:
            return (self.key == other.key and
                    self.conditions == other.conditions)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "%s(events=%r, table=%r, conditions=%r, old_conditions=%r)" % (
            self.__class__.__name__, self.events, self.table, self.conditions,
            self.old_conditions)

    @abc.abstractmethod
    def matches(self, event, row, old=None):
        """Test that `event` on `row` matches watched events

        :param event: event type
        :type event:  ROW_CREATE, ROW_UPDATE, or ROW_DELETE
        :param row:
        :param old:
        :returns:    boolean, True if match else False
        """

    @abc.abstractmethod
    def run(self, event, row, old):
        """Method to run when the event matches"""


class WaitEvent(RowEvent):
    event_name = 'WaitEvent'
    ONETIME = True

    def __init__(self, *args, **kwargs):
        self.event = threading.Event()
        self.timeout = kwargs.pop('timeout', None)
        super(WaitEvent, self).__init__(*args, **kwargs)

    @abc.abstractmethod
    def matches(self, event, row, old=None):
        """Test that `event on `row` matches watched events. See: RowEvent"""

    def run(self, event, row, old):
        self.event.set()

    def wait(self):
        return self.event.wait(self.timeout)


class RowEventHandler(object):
    def __init__(self):
        self.__watched_events = set()
        self.__lock = threading.Lock()
        self.notifications = Queue.Queue()
        self.notify_thread = threading.Thread(target=self.notify_loop)
        self.notify_thread.daemon = True
        atexit.register(self.shutdown)
        self.start()

    def start(self):
        self.notify_thread.start()

    def matching_events(self, event, row, updates):
        with self.__lock:
            return tuple(t for t in self.__watched_events
                         if t.matches(event, row, updates))

    def watch_event(self, event):
        with self.__lock:
            self.__watched_events.add(event)

    def watch_events(self, events):
        with self.__lock:
            for event in events:
                self.__watched_events.add(event)

    def unwatch_event(self, event):
        with self.__lock:
            self.__watched_events.discard(event)

    def unwatch_events(self, events):
        with self.__lock:
            for event in events:
                self.__watched_events.discard(event)

    def shutdown(self):
        self.notifications.put(STOP_EVENT)

    def notify_loop(self):
        while True:
            try:
                match, event, row, updates = self.notifications.get()
                if (match, event, row, updates) == STOP_EVENT:
                    self.notifications.task_done()
                    break
                match.run(event, row, updates)
                if match.ONETIME:
                    self.unwatch_event(match)
                self.notifications.task_done()
            except Exception:
                # If any unexpected exception happens we don't want the
                # notify_loop to exit.
                LOG.exception('Unexpected exception in notify_loop')

    def notify(self, event, row, updates=None):
        """Method for calling backend to call for each DB update

        :param event:   Backend representation of event type, e.g.
                        create, update, delete
        :param row:     Backend representation of a Row object. If it is not
                        immutable, it should be converted or guaranteed not to
                        be changed in other threads.
        :param updates: Backend representation of updates to a Row. e.g.
                        a Row object with just changed attributes, a
                        dictionary of changes, etc.
        """
        matching = self.matching_events(
            event, row, updates)
        for match in matching:
            self.notifications.put((match, event, row, updates))
