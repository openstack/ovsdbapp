#!/usr/bin/env python
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
import atexit
import os
import subprocess
import sys

from fixtures import fixture

from ovsdbapp import venv


def reraise(tp, value, tb=None):
    try:
        if value is None:
            value = tp()
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value
    finally:
        value = None
        tb = None


if len(sys.argv) != 4:
    print("Requires three arguments: venvdir ovsdir ovndir", file=sys.stderr)
    sys.exit(1)

for d in sys.argv[1:]:
    if not os.path.isdir(d):
        print("%s is not a directory" % d, file=sys.stderr)
        sys.exit(1)

venvdir = os.path.abspath(sys.argv[1])
ovsdir = os.path.abspath(sys.argv[2])
ovndir = os.path.abspath(sys.argv[3])

v = venv.OvsOvnVenvFixture(venvdir, ovsdir=ovsdir, ovndir=ovndir)
try:
    atexit.register(v.cleanUp)
    v.setUp()
except fixture.MultipleExceptions as e:
    reraise(*e.args[0])
try:
    print("*** Exit the shell when finished debugging ***")
    subprocess.call([os.getenv('SHELL'), '-i'], env=v.env)
except Exception:
    print("*** Could not start shell, don't type 'exit'***", file=sys.stderr)
    raise
