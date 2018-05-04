#!/bin/bash

set -e

## Go Tests
echo "Testing code with 'go test' ..."

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

EXTRA_FLAGS=""
if [ "$SG_TEST_BACKING_STORE" == "Couchbase" ] || [ "$SG_TEST_BACKING_STORE" == "couchbase" ]; then
    echo "Integration mode: forcing tests to run in serial across packages via -p 1 flag"
    EXTRA_FLAGS="-p 1"  # force this to run in serial, otherwise packages run in parallel and interfere with each other
fi

# Extend timeout 
EXTRA_FLAGS="$EXTRA_FLAGS -timeout=180m"

echo "Running Sync Gateway unit tests"
go test "$@" $EXTRA_FLAGS github.com/couchbase/sync_gateway/...

if [ -d godeps/src/github.com/couchbaselabs/sync-gateway-accel ]; then
    echo "Running Sync Gateway Accel unit tests"
    go test "$@" $EXTRA_FLAGS github.com/couchbaselabs/sync-gateway-accel/...
fi
