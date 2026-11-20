#!/usr/bin/env python3
# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

"""Fail unless every Jenkinsfile gives COUCHBASE_SERVER_VERSION the same default."""

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
NAME_RE = re.compile(r"""name:\s*['"]COUCHBASE_SERVER_VERSION['"]""")
PARAM_RE = re.compile(
    NAME_RE.pattern + r""",\s*(defaultValue|value):\s*['"]?([^'",\s]*)"""
)


def main() -> int:
    defaults: dict[str, str] = {}
    for path in sorted(REPO_ROOT.glob(".ci/jenkins/**/Jenkinsfile")):
        text = path.read_text()
        rel = str(path.relative_to(REPO_ROOT))
        matches = PARAM_RE.findall(text)
        # Every declaration must parse, so that a reformatted parameter is not skipped silently.
        if len(matches) != len(NAME_RE.findall(text)):
            print(
                f"{rel}: cannot parse a COUCHBASE_SERVER_VERSION parameter",
                file=sys.stderr,
            )
            return 1
        for kind, value in matches:
            if kind == "defaultValue":
                defaults[rel] = value

    if len(set(defaults.values())) > 1:
        print("COUCHBASE_SERVER_VERSION defaults differ:", file=sys.stderr)
        for rel, value in defaults.items():
            print(f"  {value}  {rel}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
