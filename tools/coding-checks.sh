#!/bin/sh
# This script is copied from neutron and adapted for ovsdbapp.
set -eu

usage () {
    echo "Usage: $0 [OPTION]..."
    echo "Run ovsdbapp's coding check(s)"
    echo ""
    echo "  -Y, --pylint [<basecommit>] Run pylint check on the entire ovsdbapp module or just files changed in basecommit (e.g. HEAD~1)"
    echo "  -h, --help                  Print this usage message"
    echo
    exit 0
}

join_args() {
    if [ -z "$scriptargs" ]; then
        scriptargs="$opt"
    else
        scriptargs="$scriptargs $opt"
    fi
}

process_options () {
    i=1
    while [ $i -le $# ]; do
        eval opt=\$$i
        case $opt in
            -h|--help) usage;;
            -Y|--pylint) pylint=1;;
            -O|--oslo) oslo=1;;
            -a|--all) oslo=1; pylint=1;;
            *) join_args;;
        esac
        i=$((i+1))
    done
}

run_oslo () {
    echo "Checking for oslo libraries in requirements.txt..."
    if grep -q "^oslo[.-]" requirements.txt; then
        echo "oslo libraries are not allowed"
        exit 1
    fi
}

run_pylint () {
    local target="${scriptargs:-all}"

    if [ "$target" = "all" ]; then
        files="ovsdbapp"
    else
        case "$target" in
            *HEAD~[0-9]*) files=$(git diff --diff-filter=AM --name-only $target -- "*.py");;
            *) echo "$target is an unrecognized basecommit"; exit 1;;
        esac
    fi

    echo "Running pylint..."
    echo "You can speed this up by running it on 'HEAD~[0-9]' (e.g. HEAD~1, this change only)..."
    if [ -n "${files}" ]; then
        pylint --rcfile=.pylintrc --output-format=colorized ${files}
    else
        echo "No python changes in this commit, pylint check not required."
        exit 0
    fi
}

scriptargs=
pylint=0
oslo=0

process_options $@

if [ $oslo -eq 1 ]; then
    run_oslo
fi
if [ $pylint -eq 1 ]; then
    run_pylint
    exit 0
fi
