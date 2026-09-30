========
ovsdbapp
========

.. image:: https://governance.openstack.org/tc/badges/ovsdbapp.svg

.. Change things from this point on

A library for creating OVSDB applications

The ovsdbapp library is useful for creating applications that communicate
via Open_vSwitch's OVSDB protocol (https://tools.ietf.org/html/rfc7047). It
wraps the Python ``ovs`` package and adds an event loop and friendly
transactions. ovsdbapp targets Unix-like systems where Open vSwitch and
``python-ovs`` are supported.

* Free software: Apache license
* Source: https://opendev.org/openstack/ovsdbapp/
* Bugs: https://bugs.launchpad.net/ovsdbapp

Features:

* An thread-based event loop for using ovs.db.Idl
* Transaction support
* Native OVSDB communication
