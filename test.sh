#!/bin/bash

set -e

## Go Tests
echo "Testing code with 'go test' ..."

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

echo "Running Sync Gateway unit tests"
go test -timeout 180m -v "$@" github.com/couchbase/sync_gateway/...

if [ -d godeps/src/github.com/couchbaselabs/sync-gateway-accel ]; then
    echo "Running Sync Gateway Accel unit tests"
    go test -timeout 180m -v "$@" github.com/couchbaselabs/sync-gateway-accel/...
fi
