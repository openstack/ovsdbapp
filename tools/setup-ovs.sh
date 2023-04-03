#!/bin/bash -xe

OVN_BRANCH=${OVN_BRANCH:-main}

if [ "$OVN_SRCDIR" -a ! -d "$OVN_SRCDIR" ]; then
    echo "Building OVN branch $OVN_BRANCH in $OVN_SRCDIR"
    mkdir -p $OVN_SRCDIR
    git clone --recurse-submodules https://github.com/ovn-org/ovn.git $OVN_SRCDIR
    pushd $OVN_SRCDIR
    git checkout $OVN_BRANCH
    pushd ovs
    ./boot.sh && PYTHON=/usr/bin/python ./configure && make -j$(($(nproc) + 1))
    popd
    ./boot.sh && PYTHON=/usr/bin/python ./configure && make -j$(($(nproc) + 1))
    popd
fi
