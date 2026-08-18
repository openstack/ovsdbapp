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
import collections
import itertools
import logging
import queue
import threading

# Several supported distros don't package sortedcontainers. For this reason
# python-ovs, which requires it for indexing support, has an in-tree copy of
# it. We already require sortedcontainers through python-ovs' dependency, so
# to avoid issues with distro packaging, we don't add sortedcontainers as a
# hard direct requirement of ovsdbapp, and use python-ovs' if it is missing.
try:
    import sortedcontainers
except ImportError:
    from ovs.compat import sortedcontainers

LOG = logging.getLogger(__name__)
STOP_EVENT = ("STOP", None, None, None)

# Queued representation of a lock acquire/lose, dispatched by notify_loop.
# It is a namedtuple (and therefore a tuple), but its type distinguishes it
# from the 4-tuple row notifications and from STOP_EVENT, so lock transitions
# ride the same notify_loop thread as row events without being mixed.
LockNotification = collections.namedtuple(
    "LockNotification", ["lock_name", "acquired"])


class RowEvent(object, metaclass=abc.ABCMeta):
    ROW_CREATE = "create"
    ROW_UPDATE = "update"
    ROW_DELETE = "delete"
    ONETIME = False
    priority = 20

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
                    self.conditions == other.conditions and
                    self.priority == other.priority)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("%s(events=%r, table=%r, conditions=%r, old_conditions=%r), "
                "priority=%d" %
                (self.__class__.__name__, self.events,
                 self.table, self.conditions, self.old_conditions,
                 self.priority))

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

    @property
    def event_name(self):
        if not hasattr(self, '_event_name'):
            return self.__class__.__name__
        return self._event_name

    @event_name.setter
    def event_name(self, value):
        self._event_name = value


class WaitEvent(RowEvent):
    ONETIME = True
    priority = 10

    def __init__(self, *args, **kwargs):
        self.event = threading.Event()
        self.timeout = kwargs.pop('timeout', None)
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def matches(self, event, row, old=None):
        """Test that `event on `row` matches watched events. See: RowEvent"""

    def run(self, event, row, old):
        self.event.set()

    def wait(self):
        return self.event.wait(self.timeout)


class RowEventHandler(object):
    def __init__(self):
        self._queues = sortedcontainers.SortedDict(lambda p: -p)
        self._lock = threading.Lock()
        self.notifications = queue.Queue()
        self.notify_thread = threading.Thread(target=self.notify_loop)
        self.notify_thread.daemon = True
        atexit.register(self.shutdown)
        self.start()

    def start(self):
        self.notify_thread.start()

    def _get_queue(self, event):
        return self._queues.setdefault(event.priority, set())

    @property
    def _watched_events(self):
        return itertools.chain(*self._queues.values())

    def _add(self, event):
        self._get_queue(event).add(event)

    def _discard(self, event):
        self._get_queue(event).discard(event)

    @staticmethod
    def match(candidate, event, row, updates):
        try:
            return candidate.matches(event, row, updates)
        except Exception:
            LOG.exception("Event not matched due to this exception:\n")
            return False

    def matching_events(self, event, row, updates):
        with self._lock:
            return tuple(t for t in self._watched_events
                         if self.match(t, event, row, updates))

    def watch_event(self, event):
        with self._lock:
            self._add(event)

    def watch_events(self, events):
        with self._lock:
            for event in events:
                self._add(event)

    def unwatch_event(self, event):
        with self._lock:
            self._discard(event)

    def unwatch_events(self, events):
        with self._lock:
            for event in events:
                self._discard(event)

    def shutdown(self):
        self.notifications.put(STOP_EVENT)

    def notify_loop(self):
        while True:
            try:
                item = self.notifications.get()
                if item == STOP_EVENT:
                    self.notifications.task_done()
                    break
                if isinstance(item, LockNotification):
                    if item.acquired:
                        self.lock_acquired(item.lock_name)
                    else:
                        self.lock_lost(item.lock_name)
                    self.notifications.task_done()
                    continue
                match, event, row, updates = item
                match.run(event, row, updates)
                if match.ONETIME:
                    self.unwatch_event(match)
                self.notifications.task_done()
            except Exception:
                # If any unexpected exception happens we don't want the
                # notify_loop to exit.
                LOG.exception('Unexpected exception in notify_loop')

    def lock_acquired(self, lock_name):
        """Hook run on the notify_loop thread when the OVSDB lock is acquired

        Override to react to acquiring the lock. Defaults to a no-op.

        :param lock_name: The name of the lock that was acquired
        """

    def lock_lost(self, lock_name):
        """Hook run on the notify_loop thread when the OVSDB lock is lost

        Override to react to losing the lock (stolen, or dropped on
        disconnect/reconnect). Defaults to a no-op.

        :param lock_name: The name of the lock that was lost
        """

    def notify_lock(self, lock_name, acquired):
        """Enqueue a lock transition for dispatch on the notify_loop thread

        The base ovs.db.idl.Idl has no lock-change callback, so ovsdbapp looks
        up an optional ``notify_lock`` attribute on the Idl. Point it here so
        the lock_acquired/lock_lost hooks run on the notify_loop thread rather
        than the connection thread::

            idl.notify_lock = handler.notify_lock

        A subclass could instead define ``notify_lock`` directly. Using an
        attribute of this name also leaves room for the callback to be adopted
        officially in upstream OVS without changing how ovsdbapp calls it.
        """
        self.notifications.put(LockNotification(lock_name, acquired))

    def _on_matching(self, event, row, updates=None):
        return row

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
        if matching:
            row = self._on_matching(event, row, updates)
        for match in matching:
            self.notifications.put((match, event, row, updates))
