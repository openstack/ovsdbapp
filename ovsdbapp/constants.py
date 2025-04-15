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


DEFAULT_OVSDB_CONNECTION = 'tcp:127.0.0.1:6640'
DEFAULT_OVNNB_CONNECTION = 'tcp:127.0.0.1:6641'
DEFAULT_TIMEOUT = 5
DEVICE_NAME_MAX_LEN = 14


ACL_PRIORITY_MAX = LR_POLICY_PRIORITY_MAX = 32767
QOS_DSCP_MAX = 2 ** 6 - 1
QOS_BANDWIDTH_MAX = 2 ** 32 - 1

NAT_SNAT = 'snat'
NAT_DNAT = 'dnat'
NAT_BOTH = 'dnat_and_snat'
NAT_TYPES = (NAT_SNAT, NAT_DNAT, NAT_BOTH)

POLICY_ACTION_ALLOW = 'allow'
POLICY_ACTION_DROP = 'drop'
POLICY_ACTION_REROUTE = 'reroute'
POLICY_ACTION_JUMP = 'jump'
POLICY_ACTION_TYPES = (POLICY_ACTION_ALLOW,
                       POLICY_ACTION_DROP,
                       POLICY_ACTION_REROUTE,
                       POLICY_ACTION_JUMP,
                       )

PROTO_TCP = 'tcp'
PROTO_UDP = 'udp'

ROUTE_DISCARD = "discard"
MAIN_ROUTE_TABLE = ""

LOCALNET = 'localnet'
DEFAULT_CHAIN = ''
