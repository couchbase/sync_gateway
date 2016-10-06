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

## Go Install Sync Gateway
echo "Building Sync Gateway with 'go install' ..."
go install "$@" github.com/couchbase/sync_gateway/...
echo "Success!"
# Let user where to know where to find binaries
if [ -f godeps/bin/sync_gateway ]; then
    echo "Sync Gateway binary compiled to: godeps/bin/sync_gateway"
fi

# Go install Sg Accel
if [ -d godeps/src/github.com/couchbaselabs/sync-gateway-accel ]; then
    echo "Building Sync Gateway Accel with 'go install' ..."
    go install "$@" github.com/couchbaselabs/sync-gateway-accel/...
    if [ -f godeps/bin/sync-gateway-accel ]; then
	echo "Sync Gateway Accel compiled to: godeps/bin/sync-gateway-accel"
    fi    
fi



