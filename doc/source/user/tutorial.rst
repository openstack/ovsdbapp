========
Tutorial
========


Open vSwitch Environment Setup
------------------------------
This tutorial will use the Open vSwitch sandbox environment from the OVS
source tree. For the sake of simplicity, we will build OVS without SSL support.
You will need git, C development tools, automake, autoconf, and libtool. See
the `Installing Open vSwitch`_ instructions for build requirements
and more detailed build instructions.

.. code-block:: shell

   git clone https://github.com/openvswitch/ovs
   cd ovs
   ./boot.sh
   ./configure --disable-ssl --enable-shared
   export OVS_SRCDIR=`pwd`
   make -j $(nproc) sandbox

Backend Setup
-------------
While the original ovs-vsctl -based backend required no setup, other backends
may. For example, the python-ovs IDL backend maintains a constant connection
to ovsdb-server and requires an IDL class to be instantiated and passed to
an OVSDBapp IDL backend Connection object.


.. code-block:: python

   import os
   from ovs.db import idl as ovs_idl
   from ovsdbapp.backend.ovs_idl import connection
   from ovsdbapp.schema.open_vswitch import impl_idl

   src_dir = os.getenv("OVS_SRCDIR")
   run_dir = os.getenv("OVS_RUNDIR", "/var/run/openvswitch")
   schema_file = os.path.join(src_dir, "vswitchd", "vswitch.ovsschema")
   db_sock = os.path.join(run_dir, "db.sock")
   remote = f"unix:{db_sock}"

   schema_helper = ovs_idl.SchemaHelper(schema_file)
   schema_helper.register_all()
   idl = ovs_idl.Idl(remote, schema_helper)
   conn = connection.Connection(idl=idl, timeout=60)

   api = impl_idl.OvsdbIdl(conn)


Using the API
-------------
Each API definition varies based on the schemas it supports and what the
app requires. There is built-in support for many common OVS and OVN-related
schemas, but it is possible that the APIs defined for these are not optimized
for a given app's use cases. It may often make sense for apps to define APIs
separate from those that are in ovsdbapp.

With that said, any api that inherits from ovsdbapp.api.API will at least
have methods defined for the standard generic OVSDB DB operations found
described in the `ovs-vsctl manpage`_ under Database Commands.

* list
* find
* get
* set
* add
* remove
* clear
* create
* destroy

They are all prefixed with db\_ (e.g. list becomes db_list) and have an
interface similar to that used by ovs-vsctl, ovn-nbctl, ovn-sbctl, etc.
db_list() and db_find() return results as lists of dicts with each dict
representing a row, with keys as the column names. Later versions added
db_list_rows() and db_find_rows() to return lists of RowView objects.

API commands that interact with the OVSDB server typically return an instance
of a subclass of ovsdbapp.api.Command. These objects hold the state of a
request that will be sent to an OVSDB server as part of a transaction. They
can be thought of as the equivalent of queries in SQL.

For a Command to be sent to the OVSDB server, it must be attached to a
transaction, and committed. For single commands, this can be done with
execute():

.. code-block:: python

   results = api.db_list("Open_vSwitch").execute(check_error=True)

This implicitly creates a transaction, adds the Command returned by db_list()
to that transaction, calls commit() on the transaction, and returns the result
that is stored on the Command object. It is the equivalent of:

.. code-block:: python

   txn = api.create_transaction(check_error=True)
   list_cmd = api.db_list("Open_vSwitch")
   txn.add(list_cmd)
   txn.commit()
   results = list_cmd.result

That API also defines transaction(), a context manager, that makes
multi-command transactions easier.

.. code-block:: python

   with api.transaction(check_error=True) as txn:
       br_cmd = txn.add(api.db_create("Bridge", name="test-br"))
       txn.add(api.db_add("Open_vSwitch", ".", "bridges", br_cmd))

There are some things to note with the above code. First, is that
Transaction.add() returns the Command object that is passed to it. In the case
of the db_create() command, the row it will create can be referenced in other
commands in the same transaction. Second, if a table is defined as having at
most one row, like the Open_vSwitch table, instead of passing its UUID, "."
can be passed. Lastly, note that we are creating a Bridge row and adding it to
the Open_vSwitch row's "bridges" field. The Bridge table is not set as a "root"
table in the Open_vSwitch schema. What this means is that if no row in a root
table references this Bridge, ovsdb-server will automatically clean up this
row. The Open_vSwitch table is a root table, so referencing the bridge in that
row prevents the bridge that was just created from being immediately removed.

.. _Installing Open vSwitch: https://docs.openvswitch.org/en/latest/intro/install/
.. _ovs-vsctl manpage: http://www.openvswitch.org/support/dist-docs/ovs-vsctl.8.html
