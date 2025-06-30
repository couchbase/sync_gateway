#!/bin/bash

set -eu
set -o pipefail

SCRIPT_NAME=$(basename "${BASH_SOURCE[0]}")
DOCKER_CONTAINER_NAME=${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME:-}
if [[ -z "${DOCKER_CONTAINER_NAME}" ]]; then
    echo "${SCRIPT_NAME} does not know how to set up scripts for non docker usage yet. If you are using docker, set ${DOCKER_CONTAINER_NAME}"
    exit 1
fi

go run ./tools/cert-manager -output-dir ./integration-test/certs -couchbase-server-mgmt-url http://127.0.0.1:8091 -couchbase-server-username Administrator -couchbase-server-password password -docker-name "${DOCKER_CONTAINER_NAME}" -dns-names localhost -couchbase-server-connection-string=couchbases://localhost
