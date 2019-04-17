#!/bin/bash
# run a python script with the following passed in args:
#   1. full path to python script
#   2. python script argument #1
#      ...
#   n. last python argument
f () {
    errcode=$? # save the exit code as the first thing done in the trap function
    echo "run_pyscript: error executing python script - error code $errcode"
    exit $errcode  # or use some other value or do return instead
}
trap f ERR
# source dx-tookit
source /usr/local/dx-toolkit/environment
script="$1"
shift 1
echo "Executing $script $@"
$script "$@"
