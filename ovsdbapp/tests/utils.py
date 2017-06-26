# -*- coding: utf-8 -*-

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

import random

from ovsdbapp import constants

# NOTE(twilson) This code was copied from the neutron/neutron-lib trees


def get_random_string(length):
    """Get a random hex string of the specified length.

    :param length: The length for the hex string.
    :returns: A random hex string of the said length.
    """
    return "{0:0{1}x}".format(random.getrandbits(length * 4), length)


def get_rand_name(max_length=None, prefix='test'):
    """Return a random string.

    The string will start with 'prefix' and will be exactly 'max_length'.
    If 'max_length' is None, then exactly 8 random characters, each
    hexadecimal, will be added. In case len(prefix) <= len(max_length),
    ValueError will be raised to indicate the problem.
    """
    return get_related_rand_names([prefix], max_length)[0]


def get_rand_device_name(prefix='test'):
    return get_rand_name(
        max_length=constants.DEVICE_NAME_MAX_LEN, prefix=prefix)


def get_related_rand_names(prefixes, max_length=None):
    """Returns a list of the prefixes with the same random characters appended

    :param prefixes: A list of prefix strings
    :param max_length: The maximum length of each returned string
    :returns: A list with each prefix appended with the same random characters
    """

    if max_length:
        length = max_length - max(len(p) for p in prefixes)
        if length <= 0:
            raise ValueError("'max_length' must be longer than all prefixes")
    else:
        length = 8
    rndchrs = get_random_string(length)
    return [p + rndchrs for p in prefixes]


def get_related_rand_device_names(prefixes):
    return get_related_rand_names(prefixes,
                                  max_length=constants.DEVICE_NAME_MAX_LEN)
