#!/bin/sh -e

# This script builds sync gateway using pinned dependencies via the repo tool
#
# - Set GOPATH and call 'go install' to compile and build Sync Gateway binaries

## Update the version stamp in the code
SG_DIR=`pwd`/godeps/src/github.com/couchbase/sync_gateway
CURRENT_DIR=`pwd`
cd $SG_DIR
./set-version-stamp.sh
cd $CURRENT_DIR

## Go Install
echo "Building code with 'go install' ..."
GOPATH=`pwd`/godeps go install "$@" github.com/couchbase/sync_gateway/...

echo "Success! Output is godeps/bin/sync_gateway and godeps/bin/sg_accel "

