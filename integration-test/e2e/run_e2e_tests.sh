#!/bin/bash
# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

# Run Couchbase Lite C e2e tests against rosmar or Couchbase Server.
set -eux -o pipefail

# Resolve the script and repository directories
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}")"  && pwd)"
REPO_DIR="$( dirname "$( dirname "${SCRIPT_DIR}")")"

# Check for required environment variables
: "${BACKING_STORE:?BACKING_STORE must be set}"
: "${COUCHBASE_LITE_TESTS_COMMIT:?COUCHBASE_LITE_TESTS_COMMIT must be set}"
: "${COUCHBASE_LITE_VERSION:?COUCHBASE_LITE_VERSION must be set}"

# Validate BACKING_STORE
if [[ "$BACKING_STORE" != "rosmar" && "$BACKING_STORE" != "cbs" ]]; then
    echo "Error: BACKING_STORE must be 'rosmar' or 'cbs', found: $BACKING_STORE"
    exit 1
fi

if [[ "$BACKING_STORE" == "cbs" ]]; then
    : "${COUCHBASE_SERVER_VERSION:?COUCHBASE_SERVER_VERSION must be set when BACKING_STORE=cbs}"
    CBS_ENV_FILE="$(mktemp)"
    "${REPO_DIR}/integration-test/start_cbs.py" --version "${COUCHBASE_SERVER_VERSION}" --purpose sync_gateway_e2e --env-file "${CBS_ENV_FILE}"
    # shellcheck disable=SC1090
    source "${CBS_ENV_FILE}"
    rm -f "${CBS_ENV_FILE}"
fi

export GIT_CONFIG_GLOBAL="${SCRIPT_DIR}/.gitconfig.e2e"
export GOPRIVATE="github.com/couchbaselabs/go-fleecedelta"
git config --global url.git@github.com:couchbaselabs/go-fleecedelta.insteadOf https://github.com/couchbaselabs/go-fleecedelta
git config --global filter.lfs.required true
git config --global filter.lfs.clean "git-lfs clean -- %f"
git config --global filter.lfs.smudge "git-lfs smudge -- %f"
git config --global filter.lfs.process "git-lfs filter-process"

# Clean up any existing clone to make local re-runs idempotent
rm -rf couchbase-lite-tests

git clone --recurse-submodules https://github.com/couchbaselabs/couchbase-lite-tests.git
cd couchbase-lite-tests
git fetch origin -- "${COUCHBASE_LITE_TESTS_COMMIT}"
git checkout --detach FETCH_HEAD
git submodule sync --recursive
git submodule update --init --recursive --force

# start_local.py builds/starts the test server and Sync Gateway in one go, and (for
# --server cbs) patches the cbltest topology config's Couchbase Server hostname with
# --connstr, since cbdinocluster clusters are reachable at their docker-network
# address, not localhost. cbltest's CouchbaseServer accepts either a bare host or a
# full connection string in the "hostname" field.
START_LOCAL_ARGS=(--server "${BACKING_STORE}" --build-testserver "${COUCHBASE_LITE_VERSION}" --repo-path "${REPO_DIR}")
if [[ "$BACKING_STORE" == "cbs" ]]; then
    START_LOCAL_ARGS+=(--connstr "${SG_TEST_COUCHBASE_SERVER_URL}")
fi
uv run -- ./environment/local/start_local.py "${START_LOCAL_ARGS[@]}"

TOPOLOGY_CONFIG="$(cat environment/local/topology_config)"
# shellcheck disable=SC2086
uv run pytest --config "${TOPOLOGY_CONFIG}" --junitxml="tests/junit_report.xml" -o junit_logging=all -o junit_log_passing_tests=false "./tests" ${PYTEST_EXTRA_ARGS:-}
