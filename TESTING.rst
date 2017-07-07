ovsdbapp testing
================

Test preferences
----------------
Most ovsdbapp tests will be functional tests. Unit tests are reserved primarily
for functions that produce a given output for their inputs, regardless of
externalities. Unit tests using mock are acceptable if something is hard to
test without it. BUT, please think carefully before writing a test that makes
heavy use of mock.assert_called_once_with() as those tests *very* often tend
to test what a function *currently does* and not what a function *should do*.


Running tests
-------------
Tests are run with tox. Generally in the form of:

.. code-block:: shell

    tox -e <test-type>


Functional tests
----------------
Run the functional tests with:

.. code-block:: shell

    tox -e functional

The ovsdbapp functional tests create an OVS virtual environment similar to a
Python virtualenv. OVS will be checked out from git and placed in
.tox/functional/src/ovs and a virtual environment directory will be created.
Various OVS servers will be launched and will store their runtime files in
the virtual environment directory. The tests will then be run against these
servers. Upon test completion, the servers will be killed and the virtual
environment directory deleted. Note that one environment is created per test
process, by default one per-core.

In the event that debugging tests is necessary, it is possible to keep the
virtual environment directories by running:

.. code-block:: shell

    KEEP_VENV=1 tox -e functional

This will also write an informational file .tox/functional/ovsvenv.$pid for
each process. The first line of this file is the virtual environment directory
and additional lines are the tests run by this process. To load an OVS
virtualenv for debugging for a particular test (e.g. test_ls_add_name), call:

.. code-block:: shell

    tools/debug_venv test_ls_add_name

This will spawn a shell where you can run ovs-vsctl, ovn-nbctl, etc. on the db
used by the test. When finished, type 'exit'. See the debug_venv help for more
options.
