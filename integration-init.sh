#!/bin/sh

set -u
set -e # Abort on errors
set -x # Output all executed shell commands

CB_HTTP_ADDR='http://localhost:8091'
CB_USERNAME='Administrator'
CB_PASSWORD='password'
CB_NODE_HOSTNAME='127.0.0.1'
CB_MEM_QUOTA_BYTES='4096'
CB_INDEX_MEM_QUOTA_BYTES='1024'

# Test to see if Couchbase Server is up
# Each retry min wait 5s, max 10s. Retry 20 times with exponential backoff (delay 0), fail at 120s
curl --retry-all-errors --connect-timeout 5 --max-time 10 --retry 20 --retry-delay 0 --retry-max-time 120 "${CB_HTTP_ADDR}"

# Set up CBS
curl -u "${CB_USERNAME}:${CB_PASSWORD}" -v -X POST "${CB_HTTP_ADDR}/nodes/self/controller/settings" -d 'path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&' -d 'index_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&' -d 'cbas_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&' -d 'eventing_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&'
curl -u "${CB_USERNAME}:${CB_PASSWORD}" -v -X POST "${CB_HTTP_ADDR}/node/controller/rename" -d "hostname=${CB_NODE_HOSTNAME}"
curl -u "${CB_USERNAME}:${CB_PASSWORD}" -v -X POST "${CB_HTTP_ADDR}/node/controller/setupServices" -d 'services=kv%2Cn1ql%2Cindex'
curl -u "${CB_USERNAME}:${CB_PASSWORD}" -v -X POST "${CB_HTTP_ADDR}/pools/default" -d "memoryQuota=${CB_MEM_QUOTA_BYTES}" -d "indexMemoryQuota=${CB_INDEX_MEM_QUOTA_BYTES}"
curl -u "${CB_USERNAME}:${CB_PASSWORD}" -v -X POST "${CB_HTTP_ADDR}/settings/web" -d 'password=password&username=Administrator&port=SAME'
curl -u "${CB_USERNAME}:${CB_PASSWORD}" -v -X POST "${CB_HTTP_ADDR}/settings/indexes" -d 'indexerThreads=4' -d 'logLevel=verbose' -d 'maxRollbackPoints=10' -d 'storageMode=plasma' -d 'memorySnapshotInterval=150' -d 'stableSnapshotInterval=40000'
