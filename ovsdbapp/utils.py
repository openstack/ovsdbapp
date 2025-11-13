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

import uuid

import netaddr


# NOTE(twilson) Clearly these are silly, but they are good enough for now
# I'm happy for someone to replace them with better parsing


def normalize_ip(ip):
    return str(netaddr.IPAddress(ip, flags=netaddr.INET_ATON))


def normalize_ip_port(ipport):
    try:
        return normalize_ip(ipport)
    except netaddr.AddrFormatError as e:
        # maybe we have a port
        if ipport[0] == '[':
            # Should be an IPv6 w/ port
            try:
                ip, port = ipport[1:].split(']:')
            except ValueError as e2:
                raise netaddr.AddrFormatError("Invalid Port") from e2
            ip = "[%s]" % normalize_ip(ip)
        else:
            try:
                ip, port = ipport.split(':')
            except ValueError as e3:
                raise netaddr.AddrFormatError("Invalid Port") from e3
            ip = normalize_ip(ip)
        if int(port) <= 0 or int(port) > 65535:
            raise netaddr.AddrFormatError("Invalid port") from e
        return "%s:%s" % (ip, port)


def generate_uuid(dashed=True):
    """Create a random uuid string.

    :param dashed: Generate uuid with dashes or not
    :type dashed: bool
    :returns: string
    """
    if dashed:
        return str(uuid.uuid4())
    return uuid.uuid4().hex


def _format_uuid_string(string):
    return (string.replace('urn:', '')
                  .replace('uuid:', '')
                  .strip('{}')
                  .replace('-', '')
                  .lower())


def is_uuid_like(val):
    """Return validation of a value as a UUID.

    :param val: Value to verify
    :type val: string
    :returns: bool
    """
    try:
        return str(uuid.UUID(val)).replace('-', '') == _format_uuid_string(val)
    except (TypeError, ValueError, AttributeError):
        return False


def get_uuid(_obj):
    """Return the UUID of a UUID itself or a BaseCommand|RowView|Row object"""
    _obj = getattr(_obj, 'result', _obj)
    try:
        return _obj.uuid
    except AttributeError:
        return _obj
