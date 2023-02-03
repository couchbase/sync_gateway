#!/usr/bin/env bash
# Copyright 2022-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

set -u # error on undefined variables
set -e # Abort on errors
set -x # Output all executed shell commands

DEFAULT_GSI="true"
DEFAULT_PACKAGE_TIMEOUT="45m"
DEFAULT_SG_EDITION="EE"
DEFAULT_SG_TEST_BUCKET_POOL_DEBUG="true"
DEFAULT_SG_TEST_BUCKET_POOL_SIZE="3"
DEFAULT_TLS_SKIP_VERIFY="false"
DEFAULT_XATTRS="true"
DEFAULT_TARGET_PACKAGE="..."

CBS_NODE_COUNT=${CBS_NODE_COUNT:-1}
if [ "${1:-}" == "-m" ]; then
    echo "Running in automated master integration mode"
    # Set automated setting parameters
    SG_COMMIT="master"
    TARGET_PACKAGE=${DEFAULT_TARGET_PACKAGE}
    TARGET_TEST="ALL"
    RUN_WALRUS="true"
    USE_GO_MODULES="true"
    DETECT_RACES="false"
    SG_EDITION=${DEFAULT_SG_EDITION}
    XATTRS=${DEFAULT_XATTRS}
    RUN_COUNT="1"
    # CBS server settings
    COUCHBASE_SERVER_PROTOCOL="couchbase"
    COUCHBASE_SERVER_VERSION="enterprise-7.0.3"
    SG_TEST_BUCKET_POOL_SIZE=${DEFAULT_SG_TEST_BUCKET_POOL_SIZE}
    SG_TEST_BUCKET_POOL_DEBUG=${DEFAULT_SG_TEST_BUCKET_POOL_DEBUG}
    GSI=${DEFAULT_GSI}
    TLS_SKIP_VERIFY=${DEFAULT_TLS_SKIP_VERIFY}
    SG_CBCOLLECT_ALWAYS="false"
fi

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

if [ "${RUN_WALRUS:-}" == "true" ]; then
    # EE
    go test -coverprofile=coverage_walrus_ee.out -coverpkg=github.com/couchbase/sync_gateway/... -tags cb_sg_enterprise $GO_TEST_FLAGS github.com/couchbase/sync_gateway/${TARGET_PACKAGE:-${DEFAULT_TARGET_PACKAGE}} > verbose_unit_ee.out.raw 2>&1 | true
    # CE
    go test -coverprofile=coverage_walrus_ce.out -coverpkg=github.com/couchbase/sync_gateway/... $GO_TEST_FLAGS github.com/couchbase/sync_gateway/${TARGET_PACKAGE:-${DEFAULT_TARGET_PACKAGE}} > verbose_unit_ce.out.raw 2>&1 | true
fi

WORKSPACE_ROOT="$(pwd)"
DOCKER_CBS_ROOT_DIR="$(pwd)"
if [ "${CBS_ROOT_DIR:-}" != "" ]; then
    DOCKER_CBS_ROOT_DIR="${CBS_ROOT_DIR}"
fi

export SG_TEST_COUCHBASE_SERVER_DOCKER_NAME=couchbase
# Start CBS
docker stop ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} || true
docker rm ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} || true

DOCKER_NETWORK=couchbase-bridge
docker network rm ${DOCKER_NETWORK} || true
docker network create ${DOCKER_NETWORK}

# --volume: Makes and mounts a CBS folder for storing a CBCollect if needed
docker run -d --name ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} --network=${DOCKER_NETWORK} --volume ${DOCKER_CBS_ROOT_DIR}/cbs:/root --volume ${WORKSPACE_ROOT}:/workspace -p 8091-8096:8091-8096 -p 11207:11207 -p 11210:11210 -p 11211:11211 -p 18091-18094:18091-18094 couchbase/server:${COUCHBASE_SERVER_VERSION}

# Test to see if Couchbase Server is up
# Each retry min wait 5s, max 10s. Retry 20 times with exponential backoff (delay 0), fail at 120s
docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} curl --retry-connrefused --connect-timeout 5 --max-time 10 --retry 20 --retry-delay 0 --retry-max-time 120 'http://127.0.0.1:8091'

# Set up CBS
docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} couchbase-cli cluster-init --cluster couchbase://127.0.0.1 --cluster-username Administrator --cluster-password password --cluster-ramsize 3072 --cluster-fts-ramsize 256 --cluster-index-ramsize 3072 --services data,index,query
docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} couchbase-cli setting-index --cluster couchbase://127.0.0.1 --username Administrator --password password --index-threads 4 --index-log-level verbose --index-max-rollback-points 10 --index-log-level verbose --index-memory-snapshot-interval 40000 --index-stable-snapshot-interval 150

CBS_CLI_ARGS="-c couchbase://couchbase -u Administrator -p password"
for ((i = 1; i < ${CBS_NODE_COUNT}; i++)); do
    DOCKER_NODE_NAME="couchbase-replica$i"
    docker run --rm -d --name ${DOCKER_NODE_NAME} --network=${DOCKER_NETWORK} couchbase/server:${COUCHBASE_SERVER_VERSION}
    #docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} curl --silent --retry-connrefused --connect-timeout 5 --max-time 10 --retry 20 --retry-delay 0 --retry-max-time 120 'http://127.0.0.1:8091'
    curl --silent --retry-connrefused --connect-timeout 5 --max-time 10 --retry 20 --retry-delay 0 --retry-max-time 120 http://${DOCKER_NODE_NAME}:8091

    docker exec ${DOCKER_NODE_NAME} couchbase-cli node-init ${CBS_CLI_ARGS}
    NODE_IP=$(docker inspect --format '{{json .NetworkSettings.Networks}}' ${DOCKER_NODE_NAME} | jq -r 'first(.[]) | .IPAddress')
    docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} ping ${NODE_IP} --server-add-username Administrator --server-add-password password --services data,index,query
    docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} couchbase-cli server-add ${CBS_CLI_ARGS} --server-add ${NODE_IP} --server-add-username Administrator --server-add-password password --services data,index,query
done

# rebalance will wait for cluster to become available
docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} couchbase-cli rebalance --cluster couchbase://127.0.0.1 --username Administrator --password password --no-progress-bar

# Set up test environment variables for CBS runs
export SG_TEST_USE_XATTRS=${XATTRS:-${DEFAULT_XATTRS}}
export SG_TEST_USE_GSI=${GSI:-${DEFAULT_GSI}}
export SG_TEST_COUCHBASE_SERVER_URL="${COUCHBASE_SERVER_PROTOCOL}://127.0.0.1"
export SG_TEST_BACKING_STORE="Couchbase"
export SG_TEST_BUCKET_POOL_SIZE=${SG_TEST_BUCKET_POOL_SIZE:-${DEFAULT_SG_TEST_BUCKET_POOL_SIZE}}
export SG_TEST_BUCKET_POOL_DEBUG=${SG_TEST_BUCKET_POOL_DEBUG:-${DEFAULT_SG_TEST_BUCKET_POOL_DEBUG}}
export SG_TEST_TLS_SKIP_VERIFY=${TLS_SKIP_VERIFY:-${DEFAULT_TLS_SKIP_VERIFY}}

if [ "${SG_EDITION:-${DEFAULT_SG_EDITION}}" == "EE" ]; then
    GO_TEST_FLAGS="${GO_TEST_FLAGS} -tags cb_sg_enterprise"
fi

go test ${GO_TEST_FLAGS} -coverprofile=coverage_int.out -coverpkg=github.com/couchbase/sync_gateway/... github.com/couchbase/sync_gateway/${TARGET_PACKAGE:-${DEFAULT_TARGET_PACKAGE}} 2>&1 | stdbuf -oL tee "${INT_LOG_FILE_NAME}.out.raw" | stdbuf -oL grep -a -E '(--- (FAIL|PASS|SKIP):|github.com/couchbase/sync_gateway(/.+)?\t|TEST: |panic: )'
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
if [ "${RUN_WALRUS:-}" == "true" ]; then
    # Strip non-printable characters before xml creation
    LC_CTYPE=C tr -dc [:print:][:space:] < "verbose_unit_ee.out.raw" > "verbose_unit_ee.out"
    LC_CTYPE=C tr -dc [:print:][:space:] < "verbose_unit_ce.out.raw" > "verbose_unit_ce.out"
    ~/go/bin/go2xunit -input "verbose_unit_ee.out" -output "verbose_unit_ee.xml"
    ~/go/bin/go2xunit -input "verbose_unit_ce.out" -output "verbose_unit_ce.xml"
fi

# Get coverage
~/go/bin/gocov convert "coverage_int.out" | ~/go/bin/gocov-xml > coverage_int.xml
if [ "${RUN_WALRUS:-}" == "true" ]; then
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
