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
set -e # Abort on errors

if [ "${1:-}" == "-m" ]; then
    echo "Running in automated master integration mode"
    # Set automated setting parameters
    TARGET_PACKAGE="..."
    TARGET_TEST="ALL"
    RUN_WALRUS="true"
    DETECT_RACES="false"
    SG_EDITION="EE"
    RUN_COUNT="1"
    # CBS server settings
    COUCHBASE_SERVER_PROTOCOL="couchbase"
    COUCHBASE_SERVER_VERSION="enterprise-7.6.6"
    export SG_TEST_BUCKET_POOL_DEBUG="true"
    GSI="true"
    TLS_SKIP_VERIFY="false"
    SG_CBCOLLECT_ALWAYS="false"
fi

REQUIRED_VARS=(
    COUCHBASE_SERVER_PROTOCOL
    COUCHBASE_SERVER_VERSION
    GSI
    TARGET_TEST
    TARGET_PACKAGE
    TLS_SKIP_VERIFY
    SG_EDITION
)

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var:-}" ]; then
        echo "${var} environment variable is required to be set."
        exit 1
    fi
done
# Use Git SSH and define private repos
git config --global url.git@github.com:couchbaselabs/go-fleecedelta.insteadOf https://github.com/couchbaselabs/go-fleecedelta
export GOPRIVATE=github.com/couchbaselabs/go-fleecedelta

# Print commit
SG_COMMIT_HASH=$(git rev-parse HEAD)
echo "Sync Gateway git commit hash: $SG_COMMIT_HASH"

GO_VERSION=go$(go list -m -f '{{.GoVersion}}')
echo "Sync Gateway go.mod version is ${GO_VERSION}"
go install "golang.org/dl/${GO_VERSION}@latest"
~/go/bin/"${GO_VERSION}" download
GOROOT=$(~/go/bin/"${GO_VERSION}" env GOROOT)
PATH=${GOROOT}/bin:$PATH

echo "Downloading tool dependencies..."
go install gotest.tools/gotestsum@latest

if [ "${SG_TEST_X509:-}" == "true" ] && [ "${COUCHBASE_SERVER_PROTOCOL}" != "couchbases" ]; then
    echo "Setting SG_TEST_X509 requires using couchbases:// protocol, aborting integration tests"
    exit 1
fi

# Set environment vars
GO_TEST_FLAGS=(-v -p 1 "-count=${RUN_COUNT:-1}")
INT_LOG_FILE_NAME="verbose_int"

export PATH=$PATH:~/go/bin
echo "PATH: $PATH"

if [ "${TEST_DEBUG:-}" == "true" ]; then
    export SG_TEST_LOG_LEVEL="debug"
    export SG_TEST_BUCKET_POOL_DEBUG="true"
fi

if [ "${DISABLE_REV_CACHE:-}" == "true" ]; then
    export SG_TEST_DISABLE_REV_CACHE="true"
fi

if [ "${TARGET_TEST}" != "ALL" ]; then
    GO_TEST_FLAGS+=(-run "${TARGET_TEST}")
fi

if [ "${PACKAGE_TIMEOUT:-}" != "" ]; then
    GO_TEST_FLAGS+=(-test.timeout="${PACKAGE_TIMEOUT}")
else
    echo "Defaulting package timeout to ${DEFAULT_PACKAGE_TIMEOUT}"
    GO_TEST_FLAGS+=(-test.timeout="${DEFAULT_PACKAGE_TIMEOUT}")
fi

if [ "${DETECT_RACES:-}" == "true" ]; then
    GO_TEST_FLAGS+=(-race)
fi

if [ "${FAIL_FAST:-}" == "true" ]; then
    GO_TEST_FLAGS+=(-failfast)
fi

if [ "${SG_TEST_PROFILE_FREQUENCY:-}" == "true" ]; then
    export SG_TEST_PROFILE_FREQUENCY=${SG_TEST_PROFILE_FREQUENCY}
fi

if [ "${RUN_WALRUS}" == "true" ]; then
    set +e -x
    # EE
    gotestsum --junitfile=rosmar-ee.xml --junitfile-project-name rosmar-EE --junitfile-testcase-classname relative --format standard-verbose -- -coverprofile=coverage_walrus_ee.out -coverpkg=github.com/couchbase/sync_gateway/... -tags cb_sg_devmode,cb_sg_enterprise "${GO_TEST_FLAGS[@]}" "github.com/couchbase/sync_gateway/${TARGET_PACKAGE}" > verbose_unit_ee.out 2>&1
    xmlstarlet ed -u '//testcase/@classname' -x 'concat("rosmar-EE-", .)' rosmar-ee.xml > verbose_unit_ee.xml
    # CE
    gotestsum --junitfile=rosmar-ce.xml --junitfile-project-name rosmar-CE --junitfile-testcase-classname relative --format standard-verbose -- -coverprofile=coverage_walrus_ce.out -coverpkg=github.com/couchbase/sync_gateway/... -tags cb_sg_devmode "${GO_TEST_FLAGS[@]}" "github.com/couchbase/sync_gateway/${TARGET_PACKAGE}" > verbose_unit_ce.out 2>&1
    xmlstarlet ed -u '//testcase/@classname' -x 'concat("rosmar-CE-", .)' rosmar-ce.xml > verbose_unit_ce.xml
    set -e +x
fi

# Run CBS
if [ "${MULTI_NODE:-}" == "true" ]; then
    # multi node
    ./integration-test/start_server.sh -m "${COUCHBASE_SERVER_VERSION}"
    export SG_TEST_BUCKET_NUM_REPLICAS=1
else
    # single node
    ./integration-test/start_server.sh "${COUCHBASE_SERVER_VERSION}"
    export SG_TEST_COUCHBASE_SERVER_DOCKER_NAME="couchbase"
fi

# Set up test environment variables for CBS runs
export SG_TEST_USE_GSI=${GSI}
export SG_TEST_COUCHBASE_SERVER_URL="${COUCHBASE_SERVER_PROTOCOL}://127.0.0.1"
export SG_TEST_BACKING_STORE="Couchbase"
export SG_TEST_TLS_SKIP_VERIFY=${TLS_SKIP_VERIFY}

if [ "${SG_EDITION}" == "EE" ]; then
    GO_TEST_FLAGS+=(-tags "cb_sg_devmode,cb_sg_enterprise")
else
    GO_TEST_FLAGS+=(-tags cb_sg_devmode)
fi

set +e -x # Output all executed shell commands
gotestsum --junitfile=integration.xml --junitfile-project-name integration --junitfile-testcase-classname relative --format standard-verbose -- "${GO_TEST_FLAGS[@]}" -coverprofile=coverage_int.out -coverpkg=github.com/couchbase/sync_gateway/... "github.com/couchbase/sync_gateway/${TARGET_PACKAGE}" 2>&1 | stdbuf -oL tee "${INT_LOG_FILE_NAME}.out" | stdbuf -oL grep -a -E '(--- (FAIL|PASS|SKIP):|github.com/couchbase/sync_gateway(/.+)?\t|TEST: |panic: )'
if [ "${PIPESTATUS[0]}" -ne "0" ]; then # If test exit code is not 0 (failed)
    echo "Go test failed! Parsing logs to find cause..."
    TEST_FAILED=true
fi

set +x # Stop outputting all executed shell commands

# Collect CBS logs if server error occurred
if [ "${SG_CBCOLLECT_ALWAYS:-}" == "true" ] || grep -a -q "server logs for details\|Timed out after 1m0s waiting for a bucket to become available" "${INT_LOG_FILE_NAME}.out"; then
    docker exec -t couchbase /opt/couchbase/bin/cbcollect_info /workspace/cbcollect.zip
fi

# If rosmar tests were run, then prepend classname with integration to tell them apart
if [ "${RUN_WALRUS}" == "true" ]; then
    xmlstarlet ed -u '//testcase/@classname' -x 'concat("integration-EE-", .)' integration.xml > "${INT_LOG_FILE_NAME}.xml"
else
    cp integration.xml "${INT_LOG_FILE_NAME}.xml"
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
