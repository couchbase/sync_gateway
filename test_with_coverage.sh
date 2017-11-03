#!/bin/bash

set -e
set -x 

## Go Tests
echo "Testing code with 'go test' ..."

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

# Make sure gocoverutil is in path
path_to_gocoverutil=$(which gocoverutil)
if [ -x "$path_to_gocoverutil" ] ; then
    echo "Using gocoverutil: $path_to_gocoverutil"
else
    echo "Please install gocoverutil by running 'go get -u github.com/AlekSi/gocoverutil'"
fi

echo "Running Sync Gateway unit tests against Walrus"
SG_TEST_USE_XATTRS="false" SG_TEST_BACKING_STORE=Walrus gocoverutil -coverprofile=cover_sg.out test -v "$@" -covermode=count github.com/couchbase/sync_gateway/...
go tool cover -html=cover_sg.out -o cover_sg.html

echo "Running Sync Gateway integraton unit tests (XATTRS=false)"
echo "Integration mode: tests to run in serial across packages by default using gocoverutil"
SG_TEST_USE_XATTRS="false" SG_TEST_BACKING_STORE=Couchbase gocoverutil -coverprofile=cover_sg_integration_xattrs_false.out test -v "$@" -covermode=count github.com/couchbase/sync_gateway/...
go tool cover -html=cover_sg_integration_xattrs_false.out -o cover_sg_integration_xattrs_false.html

echo "Running Sync Gateway integraton unit tests (XATTRS=true)"
echo "Integration mode: tests to run in serial across packages by default using gocoverutil"
SG_TEST_USE_XATTRS="true" SG_TEST_BACKING_STORE=Couchbase gocoverutil -coverprofile=cover_sg_integration_xattrs_true.out test -v "$@" -covermode=count github.com/couchbase/sync_gateway/...
go tool cover -html=cover_sg_integration_xattrs_true.out -o cover_sg_integration_xattrs_true.html


echo "Merging coverage reports"
gocoverutil -coverprofile=cover_sg_merged.out merge cover_sg.out cover_sg_integration_xattrs_false.out cover_sg_integration_xattrs_true.out
go tool cover -html=cover_sg_merged.out -o cover_sg_merged.html