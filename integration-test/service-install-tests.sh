#!/bin/bash
# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.


set -eux -o pipefail

IMAGES=(
    #"almalinux:9"
    "amazonlinux:2"
    "amazonlinux:2023"
    "debian:10"
    "debian:11"
    "debian:12"
    "redhat/ubi8"
    "redhat/ubi9"
    #"rockylinux:9"
    "ubuntu:20.04"
    "ubuntu:22.04"
    "ubuntu:24.04"

    # not technically supported
    "oraclelinux:9"
)

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
SYNC_GATEWAY_DIR=$(realpath ${SCRIPT_DIR}/..)

if [ "$(uname)" == "Darwin" ]; then
    sudo ${SYNC_GATEWAY_DIR}/integration-test/service-test.sh
fi

for IMAGE in "${IMAGES[@]}"; do
    echo "Running tests for ${IMAGE}"
    docker run --mount src=${SYNC_GATEWAY_DIR},target=/sync_gateway,type=bind ${IMAGE} /bin/bash -c "/sync_gateway/integration-test/service-test.sh"
done
