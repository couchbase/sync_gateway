#!/bin/bash

# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Generates a GitHub Actions job summary from gotestsum JSON output (test.json).
# Writes to $GITHUB_STEP_SUMMARY with pass/fail/skip counts and collapsible
# failed test output.

set -euo pipefail

MAX_OUTPUT_LINES=500
JSON_FILE="${1:-test.json}"

if [ ! -f "$JSON_FILE" ]; then
    echo "⚠️ No test results found (missing $JSON_FILE)" >> "$GITHUB_STEP_SUMMARY"
    exit 0
fi

# Warn if JSON is truncated/malformed (e.g. from OOM or timeout kill)
if ! jq -es 'true' "$JSON_FILE" > /dev/null 2>&1; then
    echo "⚠️ Warning: test results ($JSON_FILE) may be truncated — summary may be incomplete" >> "$GITHUB_STEP_SUMMARY"
fi

# Count test-level and package-level results in a single pass
eval "$(jq -sr '
    { passed:     [.[] | select(.Test != null and .Action == "pass")]  | length,
      failed:     [.[] | select(.Test != null and .Action == "fail")]  | length,
      skipped:    [.[] | select(.Test != null and .Action == "skip")]  | length,
      pkg_failed: [.[] | select(.Test == null  and .Action == "fail")] | length }
    | "PASSED=\(.passed)
FAILED=\(.failed)
SKIPPED=\(.skipped)
PKG_FAILED=\(.pkg_failed)"
' "$JSON_FILE")"

if [ "$FAILED" -gt 0 ] || [ "$PKG_FAILED" -gt 0 ]; then
    echo "## ❌ $FAILED failed, $PASSED passed, $SKIPPED skipped (${PKG_FAILED} package failure(s))" >> "$GITHUB_STEP_SUMMARY"
else
    echo "## ✅ $PASSED passed, $SKIPPED skipped" >> "$GITHUB_STEP_SUMMARY"
    exit 0
fi

echo "" >> "$GITHUB_STEP_SUMMARY"

# List each top-level failed test with its output in a collapsible section.
# Only top-level tests (no "/" in name) to avoid duplicating subtest output.
jq -s -r '
    [.[] | select(.Test != null and .Action == "fail" and (.Test | contains("/") | not))]
    | unique_by(.Package, .Test)
    | sort_by(.Package, .Test)
    | .[]
    | "\(.Package)\t\(.Test)"
' "$JSON_FILE" | while IFS=$'\t' read -r pkg test; do
    {
        echo "<details>"
        echo "<summary><strong>FAIL: $test</strong> — <code>$pkg</code></summary>"
        echo ""
        echo '```'
    } >> "$GITHUB_STEP_SUMMARY"

    # Collect output for the failed test and all its subtests
    jq -s -r --arg t "$test" --arg p "$pkg" '
        [.[] | select(
            .Package == $p
            and .Action == "output"
            and .Test != null
            and (.Test == $t or (.Test | startswith($t + "/")))
        )]
        | .[].Output // empty
    ' "$JSON_FILE" | head -"$MAX_OUTPUT_LINES" >> "$GITHUB_STEP_SUMMARY"

    {
        echo '```'
        echo "</details>"
        echo ""
    } >> "$GITHUB_STEP_SUMMARY"
done

# Show package-level failures (build/compile errors, init failures)
if [ "$PKG_FAILED" -gt 0 ]; then
    jq -s -r '
        [.[] | select(.Test == null and .Action == "fail")]
        | unique_by(.Package)
        | sort_by(.Package)
        | .[].Package
    ' "$JSON_FILE" | while read -r pkg; do
        {
            echo "<details>"
            echo "<summary><strong>FAIL: package</strong> — <code>$pkg</code></summary>"
            echo ""
            echo '```'
        } >> "$GITHUB_STEP_SUMMARY"

        jq -s -r --arg p "$pkg" '
            [.[] | select(.Package == $p and .Test == null and .Action == "output")]
            | .[].Output // empty
        ' "$JSON_FILE" | head -"$MAX_OUTPUT_LINES" >> "$GITHUB_STEP_SUMMARY"

        {
            echo '```'
            echo "</details>"
            echo ""
        } >> "$GITHUB_STEP_SUMMARY"
    done
fi
