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

# NOTE(twilson) Clearly these are silly, but they are good enough for now
# I'm happy for someone to replace them with better parsing


def normalize_ip(ip):
    return str(netaddr.IPAddress(ip))


def normalize_ip_port(ipport):
    try:
        return normalize_ip(ipport)
    except netaddr.AddrFormatError:
        # maybe we have a port
        if ipport[0] == '[':
            # Should be an IPv6 w/ port
            try:
                ip, port = ipport[1:].split(']:')
            except ValueError:
                raise netaddr.AddrFormatError("Invalid Port")
            ip = "[%s]" % normalize_ip(ip)
        else:
            try:
                ip, port = ipport.split(':')
            except ValueError:
                raise netaddr.AddrFormatError("Invalid Port")
            ip = normalize_ip(ip)
        if int(port) <= 0 or int(port) > 65535:
            raise netaddr.AddrFormatError("Invalid port")
        return "%s:%s" % (ip, port)
