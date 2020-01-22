#!/bin/bash -xe

OVS_BRANCH=${OVS_BRANCH:-master}
OVN_BRANCH=${OVN_BRANCH:-$OVS_BRANCH}

function use_new_ovn_repository {
    # If OVN_BRANCH > branch-2.12 return 0
    return $(! printf "%s\n%s" $OVN_BRANCH branch-2.12 | sort -C -V)
}

# We require at least OVS 2.7. Testing infrastructure doesn't support it yet,
# so build it. Eventually, we should run some checks to see what is actually
# installed and see if we can use it instead.
if [ "$OVS_SRCDIR" -a ! -d "$OVS_SRCDIR" ]; then
    echo "Building OVS branch $OVS_BRANCH in $OVS_SRCDIR"
    mkdir -p $OVS_SRCDIR
    git clone git://github.com/openvswitch/ovs.git $OVS_SRCDIR
    (cd $OVS_SRCDIR && git checkout $OVS_BRANCH && ./boot.sh && PYTHON=/usr/bin/python ./configure && make -j$(($(nproc) + 1)))
fi
if use_new_ovn_repository && [ "$OVN_SRCDIR" -a ! -d "$OVN_SRCDIR" ]; then
    echo "Building OVN branch $OVN_BRANCH in $OVN_SRCDIR"
    mkdir -p $OVN_SRCDIR
    git clone git://github.com/ovn-org/ovn.git $OVN_SRCDIR
    (cd $OVN_SRCDIR && git checkout $OVN_BRANCH && ./boot.sh && PYTHON=/usr/bin/python ./configure --with-ovs-source=$OVS_SRCDIR && make -j$(($(nproc) + 1)))
fi
