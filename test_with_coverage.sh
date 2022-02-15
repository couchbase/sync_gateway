#!/bin/bash

# Copyright 2017-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.


## Go Tests
echo "Testing code with 'go test' ..."

# Make sure gocoverutil is in path
path_to_gocoverutil=$(which gocoverutil)
if [ -x "$path_to_gocoverutil" ] ; then
    echo "Using gocoverutil: $path_to_gocoverutil"
else
    echo "Please install gocoverutil by running 'go get -u github.com/AlekSi/gocoverutil'"
    exit 1
fi

set -e
set -x

echo "Running Sync Gateway unit tests against Walrus"
SG_TEST_USE_XATTRS="false" SG_TEST_BACKING_STORE=Walrus gocoverutil -coverprofile=cover_sg.out test -v "$@" -covermode=count ./...
go tool cover -html=cover_sg.out -o cover_sg.html

echo "Running Sync Gateway integration unit tests (XATTRS=false)"
echo "Integration mode: tests to run in serial across packages by default using gocoverutil"
SG_TEST_USE_XATTRS="false" SG_TEST_BACKING_STORE=Couchbase gocoverutil -coverprofile=cover_sg_integration_xattrs_false.out test -v "$@" -covermode=count ./...
go tool cover -html=cover_sg_integration_xattrs_false.out -o cover_sg_integration_xattrs_false.html

echo "Running Sync Gateway integration unit tests (XATTRS=true)"
echo "Integration mode: tests to run in serial across packages by default using gocoverutil"
SG_TEST_USE_XATTRS="true" SG_TEST_BACKING_STORE=Couchbase gocoverutil -coverprofile=cover_sg_integration_xattrs_true.out test -v "$@" -covermode=count ./...
go tool cover -html=cover_sg_integration_xattrs_true.out -o cover_sg_integration_xattrs_true.html


echo "Merging coverage reports"
gocoverutil -coverprofile=cover_sg_merged.out merge cover_sg.out cover_sg_integration_xattrs_false.out cover_sg_integration_xattrs_true.out
go tool cover -html=cover_sg_merged.out -o cover_sg_merged.html