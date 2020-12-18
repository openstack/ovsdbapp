# Copyright (c) 2017 OpenStack Foundation
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


class OvsdbAppException(RuntimeError):
    """Base OvsdbApp Exception.

    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.
    """
    message = "An unknown exception occurred."

    def __init__(self, **kwargs):
        try:
            super().__init__(self.message % kwargs)
            self.msg = self.message % kwargs
        except Exception:
            if self.use_fatal_exceptions():
                raise
            # at least get the core message out if something happened
            super().__init__(self.message)

    def __str__(self):
        return self.msg

    def use_fatal_exceptions(self):
        """Is the instance using fatal exceptions.

        :returns: Always returns False.
        """
        return False


class TimeoutException(OvsdbAppException):
    message = ("Commands %(commands)s exceeded timeout %(timeout)d seconds, "
               "cause: %(cause)s")


class OvsdbConnectionUnavailable(OvsdbAppException):
    message = ("OVS database connection to %(db_schema)s failed with error: "
               "'%(error)s'. Verify that OVS and related services are "
               "available and that the relevant configuration options "
               "are correct.")
