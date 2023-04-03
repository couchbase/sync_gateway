#!/bin/bash

set -eux

REPLICA_IP=$(docker inspect --format '{{json .NetworkSettings.Networks}}' cbs-replica2 | jq -r 'first(.[]) | .IPAddress')
docker kill cbs-replica2
CLI_ARGS="-c couchbase://cbs -u Administrator -p password"
docker ps

docker exec cbs couchbase-cli failover $CLI_ARGS --server-failover $REPLICA_IP --hard --force

docker exec cbs couchbase-cli rebalance $CLI_ARGS
