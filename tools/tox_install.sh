#!/usr/bin/env bash

# Library constraint file contains this library version pin that is in conflict
# with installing the library from source. We should replace the version pin in
# the constraints file before applying it for from-source installation.

ZUUL_CLONER=/usr/zuul-env/bin/zuul-cloner
BRANCH_NAME=master
LIB_NAME=ovsdbapp
requirements_installed=$(echo "import openstack_requirements" | python 2>/dev/null ; echo $?)

set -e

CONSTRAINTS_FILE=$1
shift

install_cmd="pip install"
mydir=$(mktemp -dt "$LIB_NAME-tox_install-XXXXXXX")
trap "rm -rf $mydir" EXIT
localfile=$mydir/upper-constraints.txt
if [[ $CONSTRAINTS_FILE != http* ]]; then
    CONSTRAINTS_FILE=file://$CONSTRAINTS_FILE
fi
curl $CONSTRAINTS_FILE -k -o $localfile
install_cmd="$install_cmd -c$localfile"

if [ $requirements_installed -eq 0 ]; then
    echo "Requirements already installed; using existing package"
elif [ -x "$ZUUL_CLONER" ]; then
    pushd $mydir
    $ZUUL_CLONER --cache-dir \
        /opt/git \
        --branch $BRANCH_NAME \
        git://git.openstack.org \
        openstack/requirements
    cd openstack/requirements
    $install_cmd -e .
    popd
else
    if [ -z "$REQUIREMENTS_PIP_LOCATION" ]; then
        REQUIREMENTS_PIP_LOCATION="git+https://git.openstack.org/openstack/requirements@$BRANCH_NAME#egg=requirements"
    fi
    $install_cmd -U -e ${REQUIREMENTS_PIP_LOCATION}
fi

# This is the main purpose of the script: Allow local installation of
# the current repo. It is listed in constraints file and thus any
# install will be constrained and we need to unconstrain it.
edit-constraints $localfile -- $LIB_NAME "-e file://$PWD#egg=$LIB_NAME"

# We require at least OVS 2.7. Testing infrastructure doesn't support it yet,
# so build it. Eventually, we should run some checks to see what is actually
# installed and see if we can use it instead.
if [ "$OVS_SRCDIR" -a ! -d "$OVS_SRCDIR" ]; then
    echo "Building OVS in $OVS_SRCDIR"
    mkdir -p $OVS_SRCDIR
    git clone git://github.com/openvswitch/ovs.git $OVS_SRCDIR
    (cd $OVS_SRCDIR && ./boot.sh && PYTHON=/usr/bin/python ./configure && make -j$(($(nproc) + 1)))
fi
$install_cmd -U $*
exit $?
