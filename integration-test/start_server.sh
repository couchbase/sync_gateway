#!/bin/bash
# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

set -eux -o pipefail

function usage() {
    echo "Usage: $0 [-m] [-h] containername"
}

if [ $# -gt 2 ]; then
    echo "Expected maximally two arguments"
    exit 1
fi

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -m | --multi-node)
            MULTI_NODE=true
            shift
            ;;
        -h | --help)
            echo "Usage: $0 [-m] [-h] containername"
            exit 1
            ;;
        *)
            COUCHBASE_DOCKER_IMAGE_NAME="$1"
            shift
            ;;
    esac
done

WORKSPACE_ROOT="$(pwd)"
DOCKER_CBS_ROOT_DIR="$(pwd)"
if [ "${CBS_ROOT_DIR:-}" != "" ]; then
    DOCKER_CBS_ROOT_DIR="${CBS_ROOT_DIR}"
fi

set +e
AMAZON_LINUX_2=$(grep 'Amazon Linux 2"' /etc/os-release)
set -e
if [[ -n "${AMAZON_LINUX_2}" ]]; then
    DOCKER_COMPOSE="docker-compose" # use docker-compose v1 for Jenkins AWS Linux 2
else
    DOCKER_COMPOSE="docker compose"
fi
cd -- "${BASH_SOURCE%/*}/"
${DOCKER_COMPOSE} down || true
export SG_TEST_COUCHBASE_SERVER_DOCKER_NAME=couchbase
# Start CBS
docker stop ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} || true
docker rm ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} || true
# --volume: Makes and mounts a CBS folder for storing a CBCollect if needed

# use dockerhub if no registry is specified, allows for pre-release images from alternative registries
if [[ !"${COUCHBASE_DOCKER_IMAGE_NAME}" =~ ".*/" ]]; then
    COUCHBASE_DOCKER_IMAGE_NAME="couchbase/server:${COUCHBASE_DOCKER_IMAGE_NAME}"
fi

if [ "${MULTI_NODE:-}" == "true" ]; then
    ${DOCKER_COMPOSE} up -d --force-recreate --renew-anon-volumes --remove-orphans
else
    # single node
    docker run --rm -d --name ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} --volume "${WORKSPACE_ROOT}:/workspace" -p 8091-8096:8091-8096 -p 11207:11207 -p 11210:11210 -p 11211:11211 -p 18091-18094:18091-18094 "${COUCHBASE_DOCKER_IMAGE_NAME}"
fi

# Test to see if Couchbase Server is up
# Each retry min wait 5s, max 10s. Retry 20 times with exponential backoff (delay 0), fail at 120s
curl --retry-all-errors --connect-timeout 5 --max-time 10 --retry 20 --retry-delay 0 --retry-max-time 120 'http://127.0.0.1:8091'

# Set up CBS

docker exec couchbase couchbase-cli cluster-init --cluster-username Administrator --cluster-password password --cluster-ramsize 3072 --cluster-index-ramsize 3072 --cluster-fts-ramsize 256 --services data,index,query
docker exec couchbase couchbase-cli setting-index --cluster couchbase://localhost --username Administrator --password password --index-threads 4 --index-log-level verbose --index-max-rollback-points 10 --index-storage-setting default --index-memory-snapshot-interval 150 --index-stable-snapshot-interval 40000

curl -u Administrator:password -v -X POST http://127.0.0.1:8091/node/controller/rename -d 'hostname=127.0.0.1'

if [ "${MULTI_NODE:-}" == "true" ]; then
    REPLICA1_NAME=couchbase-replica1
    REPLICA2_NAME=couchbase-replica2
    CLI_ARGS=(-c couchbase://couchbase -u Administrator -p password)
    docker exec ${REPLICA1_NAME} couchbase-cli node-init "${CLI_ARGS[@]}"
    docker exec ${REPLICA2_NAME} couchbase-cli node-init "${CLI_ARGS[@]}"
    REPLICA1_IP=$(docker inspect --format '{{json .NetworkSettings.Networks}}' ${REPLICA1_NAME} | jq -r 'first(.[]) | .IPAddress')
    REPLICA2_IP=$(docker inspect --format '{{json .NetworkSettings.Networks}}' ${REPLICA2_NAME} | jq -r 'first(.[]) | .IPAddress')
    docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} couchbase-cli server-add "${CLI_ARGS[@]}" --server-add "$REPLICA2_IP" --server-add-username Administrator --server-add-password password --services data,index,query
    docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} couchbase-cli server-add "${CLI_ARGS[@]}" --server-add "$REPLICA1_IP" --server-add-username Administrator --server-add-password password --services data,index,query
    docker exec ${SG_TEST_COUCHBASE_SERVER_DOCKER_NAME} couchbase-cli rebalance "${CLI_ARGS[@]}"
fi
