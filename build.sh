#!/bin/sh -e

# This script builds sync gateway using pinned dependencies via the repo tool
#
# - Set GOPATH and call 'go install' to compile and build Sync Gateway binaries

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

updateVersionStamp () {

    # Build path to SG code directory
    SG_DIR=$GOPATH/src/github.com/couchbase/sync_gateway

    # Save the current directory
    CURRENT_DIR=`pwd`

    # Cd into SG code directory
    cd $SG_DIR
    ./set-version-stamp.sh

    # Go back to the original current directory
    cd $CURRENT_DIR
    
}

updateVersionStamp

## Go Install
echo "Building code with 'go install' ..."
go install "$@" github.com/couchbase/sync_gateway/...

echo "Success! Output is godeps/bin/sync_gateway and godeps/bin/sg_accel "

