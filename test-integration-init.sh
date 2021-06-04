#!/bin/bash

# Copyright 2018-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

############
## CONFIG ##
############

CB_CLI_PATH=""
CB_SERVER_URL=${SG_TEST_COUCHBASE_SERVER_URL:-http://localhost:8091}

CB_ADMIN_USERNAME="Administrator"
CB_ADMIN_PASSWORD="password"

SG_TEST_BUCKETS=("test_data_bucket" "test_indexbucket")
SG_TEST_BUCKET_RAMSIZE=200 # MB

SG_TEST_BUCKET_PASSWORD="password"
SG_TEST_BUCKET_RBAC_ROLES=() # No bucket-specific roles when we can rely on global admin
SG_TEST_GLOBAL_RBAC_ROLES=("admin")

################
## END CONFIG ##
################

set -e

cb_cli_tool="couchbase-cli"

# Tries to find couchbase-cli in common places so we can use it to initilize the buckets and RBAC users.
function find_couchbase-cli {
    set +e
    paths=("$CB_CLI_PATH" "/opt/couchbase/bin/" "/Applications/Couchbase Server.app/Contents/Resources/couchbase-core/bin/")
    for path in "${paths[@]}"; do
        "$path$cb_cli_tool" -h >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "couchbase-cli found at: $path"
            cb_cli_tool="$path$cb_cli_tool"
        fi
    done
    set -e
}

# Will attempt to remove buckets and rbac users
function cb_cleanup {
    set +e
    for bucket in "${SG_TEST_BUCKETS[@]}"; do
        "$cb_cli_tool" bucket-delete -c $CB_SERVER_URL --username $CB_ADMIN_USERNAME \
            --password $CB_ADMIN_PASSWORD --bucket=$bucket
    done
    cb_manage_rbac_users delete
    set -e
}

# will create the buckets defined at the top of the script
function cb_create_buckets {
    for bucket in "${SG_TEST_BUCKETS[@]}"; do
        "$cb_cli_tool" bucket-create -c $CB_SERVER_URL --username $CB_ADMIN_USERNAME \
            --password $CB_ADMIN_PASSWORD --bucket=$bucket --bucket-type=couchbase \
            --bucket-ramsize=$SG_TEST_BUCKET_RAMSIZE --enable-flush=1 \
            --bucket-replica=0 --enable-index-replica=0 --wait
    done
}

# will create or delete rbac users defined at the top of the script based on given parameter
function cb_manage_rbac_users {
    for bucket in "${SG_TEST_BUCKETS[@]}"; do
        if [ "$1" == "delete" ]; then
            "$cb_cli_tool" user-manage -c $CB_SERVER_URL --username $CB_ADMIN_USERNAME \
            --password $CB_ADMIN_PASSWORD --delete --rbac-username $bucket --auth-domain local
        elif [ "$1" == "create" ]; then
            # Build up a string in the format of "globalrole,bucketrole1[bucketname],bucketrole2[bucketname]"
            roles=""
            for role in "${SG_TEST_GLOBAL_RBAC_ROLES[@]}"; do
                roles="$roles$role,"
            done
            for role in "${SG_TEST_BUCKET_RBAC_ROLES[@]}"; do
                roles="$roles$role[$bucket],"
            done
            # Trim the trailing comma
            roles=${roles%?}
            
            "$cb_cli_tool" user-manage -c $CB_SERVER_URL --username $CB_ADMIN_USERNAME \
                --password $CB_ADMIN_PASSWORD --set --rbac-username $bucket \
                --rbac-password $SG_TEST_BUCKET_PASSWORD --roles=$roles --auth-domain local
        else
            echo "unrecognised cb_manage_rbac_users argument: $1"
            exit 1
        fi
    done
}

if [ "$SG_TEST_BACKING_STORE_RECREATE" == "true" ]; then
    echo "SG_TEST_BACKING_STORE_RECREATE set, re-creating buckets and RBAC users"
    find_couchbase-cli
    cb_cleanup
    cb_create_buckets
    cb_manage_rbac_users create
else
    echo "SG_TEST_BACKING_STORE_RECREATE not set, skipping bucket/RBAC user re-creation"
fi
