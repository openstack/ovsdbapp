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


try:
    import eventlet
    from eventlet import tpool
except ImportError:
    eventlet = None


def avoid_blocking_call(f, *args, **kwargs):
    """Ensure that the method "f" will not block other greenthreads.

    Performs the call to the function "f" received as parameter in a
    different thread using tpool.execute when called from a greenthread.
    This will ensure that the function "f" will not block other greenthreads.
    If not called from a greenthread, it will invoke the function "f" directly.
    The function "f" will receive as parameters the arguments "args" and
    keyword arguments "kwargs". If eventlet is not installed on the system
    then this will call directly the function "f".
    """
    if eventlet is None:
        return f(*args, **kwargs)

    # Note that eventlet.getcurrent will always return a greenlet object.
    # In case of a greenthread, the parent greenlet will always be the hub
    # loop greenlet.
    if eventlet.getcurrent().parent:
        return tpool.execute(f, *args, **kwargs)
    return f(*args, **kwargs)
