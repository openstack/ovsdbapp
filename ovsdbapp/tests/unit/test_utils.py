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

import netaddr
from ovs.db import idl
from unittest import mock
import uuid

from ovsdbapp.backend.ovs_idl import command
from ovsdbapp.backend.ovs_idl import rowview
from ovsdbapp.tests import base
from ovsdbapp import utils


class TestUtils(base.TestCase):

    def test_normalize_ip(self):
        good = [
            ('4.4.4.4', '4.4.4.4'),
            ('10.0.0.0', '10.0.0.0'),
            ('123', '0.0.0.123'),
            ('2001:0db8:85a3:0000:0000:8a2e:0370:7334',
             '2001:db8:85a3::8a2e:370:7334')
        ]
        bad = ('256.1.3.2', 'bad', '192.168.1.1:80')
        for before, after in good:
            norm = utils.normalize_ip(before)
            self.assertEqual(after, norm,
                             "%s does not match %s" % (after, norm))
        for val in bad:
            self.assertRaises(netaddr.AddrFormatError, utils.normalize_ip, val)

    def test_normalize_ip_port(self):
        good = [
            ('4.4.4.4:53', '4.4.4.4:53'),
            ('10.0.0.0:7', '10.0.0.0:7'),
            ('123:12', '0.0.0.123:12'),
            ('[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:80',
             '[2001:db8:85a3::8a2e:370:7334]:80')
        ]
        bad = ('1.2.3.4:0', '1.2.3.4:99000',
               '2001:0db8:85a3:0000:0000:8a2e:0370:7334:80')
        for before, after in good:
            norm = utils.normalize_ip_port(before)
            self.assertEqual(after, norm,
                             "%s does not match %s" % (after, norm))
        for val in bad:
            self.assertRaises(netaddr.AddrFormatError,
                              utils.normalize_ip_port, val)

    def test_is_uuid_like(self):
        self.assertTrue(utils.is_uuid_like(str(uuid.uuid4())))
        self.assertTrue(utils.is_uuid_like(
            '{12345678-1234-1234-1234-123456781234}'))
        self.assertTrue(utils.is_uuid_like(
            '12345678123412341234123456781234'))
        self.assertTrue(utils.is_uuid_like(
            'urn:uuid:12345678-1234-1234-1234-123456781234'))
        self.assertTrue(utils.is_uuid_like(
            'urn:bbbaaaaa-aaaa-aaaa-aabb-bbbbbbbbbbbb'))
        self.assertTrue(utils.is_uuid_like(
            'uuid:bbbaaaaa-aaaa-aaaa-aabb-bbbbbbbbbbbb'))
        self.assertFalse(utils.is_uuid_like(
            'uuid:batrdbaa-aaaa-aaaa-aabb-bbbbbbbbbbbb'))
        self.assertFalse(utils.is_uuid_like(
            '123456781234123412341234567812345678'))

    def test_get_uuid(self):
        uuid_obj = uuid.uuid4()
        row_obj = idl.Row(mock.Mock(), 'table', uuid_obj, 'data')
        rowview_obj = rowview.RowView(row_obj)
        command_obj = command.BaseCommand(mock.Mock())
        command_obj.result = row_obj
        for tested_obj in (uuid_obj,
                           row_obj,
                           rowview_obj,
                           command_obj):
            self.assertEqual(uuid_obj, utils.get_uuid(tested_obj))
