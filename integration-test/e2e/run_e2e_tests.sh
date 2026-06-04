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
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_DIR="$( dirname "$( dirname "${SCRIPT_DIR}" )" )"

# Check for required environment variables
: "${BACKING_STORE:?BACKING_STORE must be set}"
: "${COUCHBASE_LITE_TESTS_COMMIT:?COUCHBASE_LITE_TESTS_COMMIT must be set}"
: "${COUCHBASE_LITE_VERSION:?COUCHBASE_LITE_VERSION must be set}"
: "${TEST_DIRECTORY:?TEST_DIRECTORY must be set}"

# Validate BACKING_STORE
if [[ "$BACKING_STORE" != "rosmar" && "$BACKING_STORE" != "cbs" ]]; then
    echo "Error: BACKING_STORE must be 'rosmar' or 'cbs', found: $BACKING_STORE"
    exit 1
fi

if [[ "$BACKING_STORE" == "cbs" ]]; then
    : "${COUCHBASE_SERVER_VERSION:?COUCHBASE_SERVER_VERSION must be set when BACKING_STORE=cbs}"
    "${REPO_DIR}/integration-test/start_server.sh" "${COUCHBASE_SERVER_VERSION}"
fi

export GIT_CONFIG_GLOBAL="${SCRIPT_DIR}/.gitconfig.e2e"
export GOPRIVATE="github.com/couchbaselabs/go-fleecedelta"
git config --global url.git@github.com:couchbaselabs/go-fleecedelta.insteadOf https://github.com/couchbaselabs/go-fleecedelta

# Clean up any existing clone to make local re-runs idempotent
rm -rf couchbase-lite-tests

git clone --recurse-submodules https://github.com/couchbaselabs/couchbase-lite-tests.git
cd couchbase-lite-tests
git checkout --detach "${COUCHBASE_LITE_TESTS_COMMIT}"
git submodule sync --recursive
git submodule update --init --recursive --force

uv run -- ./environment/local/start_local.py --build-testserver "${COUCHBASE_LITE_VERSION}"
uv run -- ./environment/local/build_sync_gateway.py --repo-path "${REPO_DIR}"
uv run -- ./environment/local/run_sync_gateway.py --start --server "${BACKING_STORE}"
# shellcheck disable=SC2086
uv run pytest --config "./environment/local/${BACKING_STORE}_config.json" --junitxml="${TEST_DIRECTORY}/junit_report.xml" -o junit_logging=all -o junit_log_passing_tests=false "./${TEST_DIRECTORY}" ${PYTEST_EXTRA_ARGS:-}
