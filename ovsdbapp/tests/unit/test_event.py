# Copyright (c) 2017 Red Hat, Inc.
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

from unittest import mock

from ovsdbapp import event
from ovsdbapp.tests import base


class TestEvent(event.RowEvent):
    def __init__(self, events=(event.RowEvent.ROW_CREATE,),
                 table="FakeTable", conditions=(("col", "=", "val"),),
                 priority=None):
        super().__init__(events, table, conditions)
        if priority is not None:
            self.priority = priority

    def run(self):
        pass

    def matches(self):
        pass


class OtherTestEvent(TestEvent):
    pass


class TestRowEvent(base.TestCase):
    def test_compare_stop_event(self):
        r = TestEvent()
        self.assertNotEqual((r, "fake", "fake", "fake"), event.STOP_EVENT)

    def test_compare_equality(self):
        self.assertEqual(TestEvent(), TestEvent())
        self.assertNotEqual(TestEvent(), TestEvent(table="NotFaketable"))
        self.assertNotEqual(TestEvent(), TestEvent(conditions=None))
        self.assertNotEqual(TestEvent(priority=1), TestEvent(priority=2))
        self.assertNotEqual(TestEvent(), OtherTestEvent())


class TestRowEventHandler(base.TestCase):
    def setUp(self):
        super().setUp()
        self.handler = event.RowEventHandler()
        self.assertEqual(0, len(tuple(self.handler._watched_events)))

    def test_watch_event(self):
        event = TestEvent()
        expected = (event,)
        self.handler.watch_event(event)
        self.assertCountEqual(expected, self.handler._watched_events)
        return expected

    def test_watch_events(self):
        events = [TestEvent(priority=r) for r in range(10)]
        expected = list(reversed(events))
        self.handler.watch_events(events)
        self.assertEqual(expected, list(self.handler._watched_events))
        return expected

    def test_unwatch_event(self):
        expected = self.test_watch_events()
        removed = expected.pop(5)
        self.handler.unwatch_event(removed)
        self.assertEqual(expected, list(self.handler._watched_events))

    def test_unwatch_events(self):
        expected = self.test_watch_events()
        removed = [expected.pop(5), expected.pop(7)]
        self.handler.unwatch_events(removed)
        self.assertEqual(expected, list(self.handler._watched_events))

    def test_add_duplicate(self):
        self.handler.watch_event(TestEvent())
        self.handler.watch_event(TestEvent())
        self.assertCountEqual(self.handler._watched_events, [TestEvent()])


class TestRowEventHandlerNotifyLoop(base.TestCase):
    @mock.patch.object(event.RowEventHandler, 'start')
    def setUp(self, mock_start):
        super().setUp()
        # Run the loop synchronously with a real queue and a stop marker.
        self.handler = event.RowEventHandler()
        self.addCleanup(event.atexit.unregister, self.handler.shutdown)

    def _test_notify_loop_exception(self, notification, callback, *args):
        callback.side_effect = [RuntimeError('callback failed'), None]
        self.handler.notifications.put(notification)
        self.handler.notifications.put(notification)
        self.handler.shutdown()

        with mock.patch.object(event.LOG, 'exception') as log_exception:
            self.handler.notify_loop()

        # A failed callback must not prevent the next notification running.
        self.assertEqual([mock.call(*args), mock.call(*args)],
                         callback.call_args_list)
        log_exception.assert_called_once_with(
            'Unexpected exception in notify_loop')
        # Checking the count avoids hanging Queue.join() on a regression.
        self.assertEqual(0, self.handler.notifications.unfinished_tasks)

    def test_row_exception(self):
        row_event = mock.Mock(ONETIME=False)
        args = (event.RowEvent.ROW_UPDATE, mock.sentinel.row,
                mock.sentinel.updates)
        self._test_notify_loop_exception((row_event, *args), row_event.run,
                                         *args)

    def test_lock_acquired_exception(self):
        self.handler.lock_acquired = mock.Mock()
        self._test_notify_loop_exception(
            event.LockNotification('test_lock', True),
            self.handler.lock_acquired, 'test_lock')

    def test_lock_lost_exception(self):
        self.handler.lock_lost = mock.Mock()
        self._test_notify_loop_exception(
            event.LockNotification('test_lock', False),
            self.handler.lock_lost, 'test_lock')

    @mock.patch.object(event.LOG, 'exception')
    def test_get_exception(self, log_exception):
        with mock.patch.object(
                self.handler.notifications, 'get',
                side_effect=[RuntimeError('get failed'), event.STOP_EVENT]), \
                mock.patch.object(self.handler.notifications,
                                  'task_done') as task_done:
            self.handler.notify_loop()

        # Only the successfully retrieved stop marker is acknowledged.
        task_done.assert_called_once_with()
        log_exception.assert_called_once_with(
            'Unexpected exception in notify_loop')
