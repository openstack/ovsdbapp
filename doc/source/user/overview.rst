========
Overview
========

OVSDBapp is a library to make it easier to write applications that interact
with an Open vSwitch database server. It allows the user to separate support
for a particular OVSDB schema and the backend method of communication with the
OVSDB server.

OVSDBapp Concepts
-----------------

API
  The interface that an application will use for reading or modifying entries
  in the OVS database. Whatever backend communication method is used, as long
  as user code only accesses methods in this API, no user code should need to
  be changed when swapping between backends.
Backend
  The Backend handles the communication with Open vSwitch. Originally, there
  were two OVSDBapp backends: 1) one that used the ovs-vsctl CLI utility to
  interact with the OVS database and 2) one that maintains a persistent
  connection to an OVSDB server using the python-ovs library. Currently, only
  the python-ovs backend is being maintained.
Command
  OVSDBapp uses the `Command Pattern`_ to isolate individual units of work
  that will be run as part of an OVSDB transaction.
Event
  OVSDB provides the ability to monitor database changes as they happen.
  OVSDBapp backends each implement the :code:`RowEvent` and
  :code:`RowEventHandler` to handle delivering these events to user code.
API Implementations:
  The backend-specific implementation of an OVSDBapp API. Only this code
  should need to be implemented to support a new backend. All other user
  code should be backend-agnostic.
Schema
  The OVSDB database schema for which the API is implemented. In current
  ovsdbapp code, the schema and API are intrinsically linked in a
  1:1 manner, but ultimately they are independent. User code could easily
  define an API specific to their application that encompasses multiple
  OVSDB schemas as long as the Backend supported it.
Transaction
  An OVSDB transaction consisting of one or more Commands.
Virtual Environment
  OVSDBapp supports running OVS and OVN services in a virtual environment.
  This is primarily used for testing.


  .. _Command Pattern: https://en.wikipedia.org/wiki/Command_pattern
