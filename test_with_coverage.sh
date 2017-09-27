#!/usr/bin/env bash

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
    exit 1
fi

# -------------------------------------------------- NEW -------------------------------------------

source integration_tests.sh

echo "array: $arr"

# Run Sync Gateway Tests
for i in "${arr[@]}"
do
    echo $i
done

echo "done"

# Run test_with_coverage.sh and generate coverage report (xattrs=false)
# gocoverutil -coverprofile=cover_sg_walrus.out test -v "$@" -covermode=count github.com/couchbase/sync_gateway/...

# Run test_integration_with_coverage.sh in Non-xattr mode and generate coverage report and merge into result
# This env variable causes the unit tests to run against Couchbase Server running on localhost.
# The tests will create any buckets they need on their own.
export SG_TEST_BACKING_STORE=Couchbase

for i in "${arr[@]}"
do
    go test -coverprofile="$i"_coverage.out -v -run ^"$i"$ github.com/couchbase/sync_gateway/...
done

# Run test_integration_with_coverage.sh in xattr mode and generate coverage report and merge into result

# Generate SG Accel coverage report




# -------------------------------------------------- OLD TEST WITH COVERAGE -------------------------------------------


#echo "Running Sync Gateway unit tests"
#gocoverutil -coverprofile=cover_sg.out test -v "$@" -covermode=count github.com/couchbase/sync_gateway/...
#
#echo "Generating Sync Gateway HTML coverage report to coverage_sync_gateway.html"
#go tool cover -html=cover_sg.out -o coverage_sync_gateway.html
#
#if [ -d godeps/src/github.com/couchbaselabs/sync-gateway-accel ]; then
#    echo "Running Sync Gateway Accel unit tests"
#    gocoverutil -coverprofile=cover_sga.out test -v "$@" -covermode=count github.com/couchbaselabs/sync-gateway-accel/...
#
#    echo "Generating SG Accel HTML coverage report to coverage_sg_accel.html"
#    go tool cover -html=cover_sga.out -o coverage_sg_accel.html
#fi


# -------------------------------------------------- OLD JENKINS JOB -------------------------------------------

#if [ "$RUN_TESTS" == "true" ]; then
#
#    echo "---------- Running unit tests ---------"
#	SG_TEST_BACKING_STORE=Walrus SG_TEST_USE_XATTRS=false ./test.sh
#fi
#
#
#if [ "$RUN_TEST_INTEGRATION" == "true" ]; then
#
#    echo "---------- Running unit integration tests ---------"
#
#    if [ ! -f test_integration.sh ]; then
#		echo "Downloading test_integration.sh"
#		curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/$SG_COMMIT/test_integration.sh" > test_integration.sh
#		chmod +x test_integration.sh
#    fi
#
#	export SG_TEST_COUCHBASE_SERVER_URL="$SG_TEST_COUCHBASE_SERVER_URL"
#    export SG_TEST_USE_XATTRS="$SG_TEST_USE_XATTRS"
#
#    ./test_integration.sh
#fi
#
#
## Generates code coverage report.
## See https://github.com/couchbase/sync_gateway/pull/2880 for details
#if [ "$TEST_COVERAGE_REPORT" == "true" ]; then
#
#    echo "---------- Generating Test Coverage Report ---------"
#
#
#    if [ ! -f test_with_coverage.sh ]; then
#		echo "Downloading test_with_coverage.sh"
#		curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/$SG_COMMIT/test_with_coverage.sh" > test_with_coverage.sh
#		chmod +x test_with_coverage.sh
#    fi
#
#    # TODO: this could also generate coverage reports with XATTRS=true
#    SG_TEST_BACKING_STORE=Walrus SG_TEST_USE_XATTRS=false ./test_with_coverage.sh
#
#fi