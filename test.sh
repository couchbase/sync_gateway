#!/bin/sh -e

# Copyright 2013-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This script tests sync gateway
#
# - Set GOPATH and call 'go test'

set -e

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

# Test both editions by default
# Limit via the $SG_EDITION env var
build_editions=( "CE" "EE" )
if [ "$SG_EDITION" = "CE" -o "$SG_EDITION" = "EE" ]; then
    echo "Testing only $SG_EDITION"
    build_editions=( $SG_EDITION )
else
    echo "Testing all editions ... Limit with 'SG_EDITION=CE $0'"
fi

doTest () {
    buildTags=""
    if [ "$1" = "EE" ]; then
        buildTags="-tags cb_sg_enterprise"
    fi

    EXTRA_FLAGS=""
    if [ "$SG_TEST_BACKING_STORE" == "Couchbase" ] || [ "$SG_TEST_BACKING_STORE" == "couchbase" ]; then
        ./test-integration-init.sh
        echo "    Integration mode: forcing -count=1 and tests to run in serial across packages via -p 1 flag"
        EXTRA_FLAGS="-count=1 -p 1" # force this to run in serial, otherwise packages run in parallel and interfere with each other
    fi

    # Default the test timeout to 20 minutes.  This means that any package will fail if the tests in that package takes longer
    # than 20 minutes to complete.
    if [ -z ${SG_TEST_TIMEOUT+x} ]; then
        echo "    Defaulting SG_TEST_TIMEOUT to 20m"
        SG_TEST_TIMEOUT="20m"
    fi

    # Extend timeout 
    EXTRA_FLAGS="$EXTRA_FLAGS -timeout=${SG_TEST_TIMEOUT}"

    echo "    Running Sync Gateway unit tests:"
    if [[ "$SG_TEST_PACKAGE" != "" ]]; then
        IFS=',' read -ra TEST <<< "$SG_TEST_PACKAGE"
        for i in "${TEST[@]}"; do
            go test ${buildTags} "${@:2}" $EXTRA_FLAGS github.com/couchbase/sync_gateway/$i
        done
    else
        go test ${buildTags} "${@:2}" $EXTRA_FLAGS github.com/couchbase/sync_gateway/...
    fi

}

for edition in "${build_editions[@]}"; do
    echo "  Testing edition: ${edition}"
    doTest $edition "$@"
done
