#!/bin/bash -xe

# We require at least OVS 2.7. Testing infrastructure doesn't support it yet,
# so build it. Eventually, we should run some checks to see what is actually
# installed and see if we can use it instead.
if [ "$OVS_SRCDIR" -a ! -d "$OVS_SRCDIR" ]; then
    echo "Building OVS in $OVS_SRCDIR"
    mkdir -p $OVS_SRCDIR
    git clone git://github.com/openvswitch/ovs.git $OVS_SRCDIR
    (cd $OVS_SRCDIR && ./boot.sh && PYTHON=/usr/bin/python ./configure && make -j$(($(nproc) + 1)))
fi
if [ "$OVN_SRCDIR" -a ! -d "$OVN_SRCDIR" ]; then
    echo "Building OVN in $OVN_SRCDIR"
    mkdir -p $OVN_SRCDIR
    git clone git://github.com/ovn-org/ovn.git $OVN_SRCDIR
    (cd $OVN_SRCDIR && ./boot.sh && PYTHON=/usr/bin/python ./configure --with-ovs-source=$OVS_SRCDIR && make -j$(($(nproc) + 1)))
fi
