#!/bin/bash -xe

# This script is triggered by extra-test-setup macro from project-config
# repository.

# Set manager for native interface
sudo ovs-vsctl --timeout=10 --id=@manager -- create Manager target=\"ptcp:6640:127.0.0.1\" -- add Open_vSwitch . manager_options @manager
