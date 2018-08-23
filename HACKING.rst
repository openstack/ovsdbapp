ovsdbapp Style Commandments
===========================

- Step 1: Read the OpenStack Style Commandments
  https://docs.openstack.org/hacking/latest/

- Step 2: Read on

ovsdbapp-specific Commandments
------------------------------

- ovsdbapp is intended to be a simple wrapper on top of python-ovs. As such,
  it must build and be deployable without any OpenStack dependencies (oslo
  projects included). It does currently use oslo.test for testing.
