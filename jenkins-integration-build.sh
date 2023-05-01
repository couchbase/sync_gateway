#!/usr/bin/env bash
# Copyright 2022-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

DEFAULT_PACKAGE_TIMEOUT="45m"

set -u

if [ "${1:-}" == "-m" ]; then
    echo "Running in automated master integration mode"
    # Set automated setting parameters
    SG_COMMIT="master"
    TARGET_PACKAGE="..."
    TARGET_TEST="ALL"
    RUN_WALRUS="true"
    USE_GO_MODULES="true"
    DETECT_RACES="false"
    SG_EDITION="EE"
    XATTRS="true"
    SG_V8="false"
    RUN_COUNT="1"
    # CBS server settings
    COUCHBASE_SERVER_PROTOCOL="couchbase"
    COUCHBASE_SERVER_VERSION="enterprise-7.0.3"
    SG_TEST_BUCKET_POOL_SIZE="3"
    SG_TEST_BUCKET_POOL_DEBUG="true"
    GSI="true"
    TLS_SKIP_VERIFY="false"
    SG_CBCOLLECT_ALWAYS="false"
fi
SG_V8="true"

set -e # Abort on errors
set -x # Output all executed shell commands

# Use Git SSH and define private repos
git config --global --replace-all url."git@github.com:".insteadOf "https://github.com/"
export GOPRIVATE=github.com/couchbaselabs/go-fleecedelta

# Print commit
SG_COMMIT_HASH=$(git rev-parse HEAD)
echo "Sync Gateway git commit hash: $SG_COMMIT_HASH"

# Use Go modules (3.1 and above) or bootstrap for legacy Sync Gateway versions (3.0 and below)
if [ "${USE_GO_MODULES:-}" == "false" ]; then
    mkdir -p sgw_int_testing # Make the directory if it does not exist
    cp bootstrap.sh sgw_int_testing/bootstrap.sh
    cd sgw_int_testing
    chmod +x bootstrap.sh
    ./bootstrap.sh -c ${SG_COMMIT} -e ee
    export GO111MODULE=off
    go get -u -v github.com/tebeka/go2xunit
    go get -u -v github.com/axw/gocov/gocov
    go get -u -v github.com/AlekSi/gocov-xml
else
    # Install tools to use after job has completed
    go install -v github.com/tebeka/go2xunit@latest
    go install -v github.com/axw/gocov/gocov@latest
    go install -v github.com/AlekSi/gocov-xml@latest
fi

if [[ -n "${JENKINS_URL:-}" ]]; then
    # last 1.x version, when updating aws linux 2 docker, docker-compose becomes docker compose
    sudo yum install -y jq
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

if [ "${SG_TEST_X509:-}" == "true" -a "${COUCHBASE_SERVER_PROTOCOL}" != "couchbases" ]; then
    echo "Setting SG_TEST_X509 requires using couchbases:// protocol, aborting integration tests"
    exit 1
fi

# Set environment vars
GO_TEST_FLAGS="-v -p 1 -count=${RUN_COUNT:-1}"
INT_LOG_FILE_NAME="verbose_int"

if [ -d "godeps" ]; then
    export GOPATH=$(pwd)/godeps
fi
export PATH=$PATH:$(go env GOPATH)/bin
echo "PATH: $PATH"

if [ "${TEST_DEBUG:-}" == "true" ]; then
    export SG_TEST_LOG_LEVEL="debug"
    export SG_TEST_BUCKET_POOL_DEBUG="true"
fi

if [ "${TARGET_TEST}" != "ALL" ]; then
    GO_TEST_FLAGS="${GO_TEST_FLAGS} -run ${TARGET_TEST}"
fi

if [ "${PACKAGE_TIMEOUT:-}" != "" ]; then
    GO_TEST_FLAGS="${GO_TEST_FLAGS} -test.timeout=${PACKAGE_TIMEOUT}"
else
    echo "Defaulting package timeout to ${DEFAULT_PACKAGE_TIMEOUT}"
    GO_TEST_FLAGS="${GO_TEST_FLAGS} -test.timeout=${DEFAULT_PACKAGE_TIMEOUT}"
fi

if [ "${DETECT_RACES:-}" == "true" ]; then
    GO_TEST_FLAGS="${GO_TEST_FLAGS} -race"
fi

if [ "${FAIL_FAST:-}" == "true" ]; then
    GO_TEST_FLAGS="${GO_TEST_FLAGS} -failfast"
fi

if [ "${SG_TEST_PROFILE_FREQUENCY:-}" == "true" ]; then
    export SG_TEST_PROFILE_FREQUENCY=${SG_TEST_PROFILE_FREQUENCY}
fi

if [ "${SG_V8:-}" == "true" ]; then
    BUILD_TAGS="-tags cb_sg_v8"
else
    BUILD_TAGS="-tags"
fi

if [ "${RUN_WALRUS}" == "true" ]; then
    # EE
    go test -coverprofile=coverage_walrus_ee.out -coverpkg=github.com/couchbase/sync_gateway/... ${BUILD_TAGS},cb_sg_enterprise $GO_TEST_FLAGS github.com/couchbase/sync_gateway/${TARGET_PACKAGE} > verbose_unit_ee.out.raw 2>&1 | true
    # CE
    go test -coverprofile=coverage_walrus_ce.out -coverpkg=github.com/couchbase/sync_gateway/... $GO_TEST_FLAGS github.com/couchbase/sync_gateway/${TARGET_PACKAGE} > verbose_unit_ce.out.raw 2>&1 | true
fi

# Run CBS
if [[ -z ${MULTI_NODE:-} ]]; then
    # Run CBS
    ./integration-test/start_server.sh "${COUCHBASE_SERVER_VERSION}"
else
    ./integration-test/start_server.sh -m "${COUCHBASE_SERVER_VERSION}"
    export SG_TEST_BUCKET_NUM_REPLICAS=1
fi

# Set up test environment variables for CBS runs
export SG_TEST_USE_XATTRS=${XATTRS}
export SG_TEST_USE_GSI=${GSI}
export SG_TEST_COUCHBASE_SERVER_URL="${COUCHBASE_SERVER_PROTOCOL}://127.0.0.1"
export SG_TEST_BACKING_STORE="Couchbase"
export SG_TEST_BUCKET_POOL_SIZE=${SG_TEST_BUCKET_POOL_SIZE}
export SG_TEST_BUCKET_POOL_DEBUG=${SG_TEST_BUCKET_POOL_DEBUG}
export SG_TEST_TLS_SKIP_VERIFY=${TLS_SKIP_VERIFY}

if [ "${SG_EDITION}" == "EE" ]; then
    GO_TEST_FLAGS="${GO_TEST_FLAGS} ${BUILD_TAGS},cb_sg_enterprise"
fi

go test ${GO_TEST_FLAGS} -coverprofile=coverage_int.out -coverpkg=github.com/couchbase/sync_gateway/... github.com/couchbase/sync_gateway/${TARGET_PACKAGE} 2>&1 | stdbuf -oL tee "${INT_LOG_FILE_NAME}.out.raw" | stdbuf -oL grep -a -E '(--- (FAIL|PASS|SKIP):|github.com/couchbase/sync_gateway(/.+)?\t|TEST: |panic: )'
if [ "${PIPESTATUS[0]}" -ne "0" ]; then # If test exit code is not 0 (failed)
    echo "Go test failed! Parsing logs to find cause..."
    TEST_FAILED=true
fi

# Collect CBS logs if server error occurred
if [ "${SG_CBCOLLECT_ALWAYS:-}" == "true" ] || grep -a -q "server logs for details\|Timed out after 1m0s waiting for a bucket to become available\|unambiguous timeout" "${INT_LOG_FILE_NAME}.out.raw"; then
    docker exec -t couchbase /opt/couchbase/bin/cbcollect_info /workspace/cbcollect.zip
fi

# Generate xunit test report that can be parsed by the JUnit Plugin
LC_CTYPE=C tr -dc [:print:][:space:] < ${INT_LOG_FILE_NAME}.out.raw > ${INT_LOG_FILE_NAME}.out # Strip non-printable characters
~/go/bin/go2xunit -input "${INT_LOG_FILE_NAME}.out" -output "${INT_LOG_FILE_NAME}.xml"
if [ "${RUN_WALRUS}" == "true" ]; then
    # Strip non-printable characters before xml creation
    LC_CTYPE=C tr -dc [:print:][:space:] < "verbose_unit_ee.out.raw" > "verbose_unit_ee.out"
    LC_CTYPE=C tr -dc [:print:][:space:] < "verbose_unit_ce.out.raw" > "verbose_unit_ce.out"
    ~/go/bin/go2xunit -input "verbose_unit_ee.out" -output "verbose_unit_ee.xml"
    ~/go/bin/go2xunit -input "verbose_unit_ce.out" -output "verbose_unit_ce.xml"
fi

# Get coverage
~/go/bin/gocov convert "coverage_int.out" | ~/go/bin/gocov-xml > coverage_int.xml
if [ "${RUN_WALRUS}" == "true" ]; then
    ~/go/bin/gocov convert "coverage_walrus_ee.out" | ~/go/bin/gocov-xml > "coverage_walrus_ee.xml"
    ~/go/bin/gocov convert "coverage_walrus_ce.out" | ~/go/bin/gocov-xml > "coverage_walrus_ce.xml"
fi

if [ "${TEST_FAILED:-}" = true ]; then
    # If output contained `FAIL:`
    if grep -q 'FAIL:' "${INT_LOG_FILE_NAME}.out"; then
        # Test failure, so mark as unstable
        exit 50
    else
        # Test setup failure or unrecovered panic, so mark as failed
        exit 1
    fi
fi
