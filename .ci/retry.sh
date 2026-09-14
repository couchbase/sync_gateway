#!/usr/bin/env bash

# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Runs a command, retrying with exponential backoff. CI uses this for Go module downloads, which
# fail intermittently when the module proxy is unhealthy.
#
# Usage: .ci/retry.sh <command> [args...]
# Tunable with RETRY_ATTEMPTS (default 4) and RETRY_DELAY (initial backoff seconds, default 5).

set -euo pipefail

attempts=${RETRY_ATTEMPTS:-4}
delay=${RETRY_DELAY:-5}

for ((attempt = 1; attempt <= attempts; attempt++)); do
    # capture the status here: $? after an if-block is the status of the if, not of the command
    status=0
    "$@" || status=$?
    if ((status == 0)); then
        exit 0
    fi
    if ((attempt < attempts)); then
        # diagnostics go to stderr so callers capturing stdout get the command's output only
        echo "retry: attempt ${attempt}/${attempts} of '$*' failed (exit ${status}), retrying in ${delay}s" >&2
        sleep "${delay}"
        delay=$((delay * 2))
    fi
done

echo "retry: '$*' failed after ${attempts} attempts" >&2
exit "${status}"
