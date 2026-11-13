#!/usr/bin/env python3
# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

"""Check that the published Sync Gateway packages were built from the expected commit.

Release builds replace the @PRODUCT_VERSION@ and @COMMIT_SHA@ placeholders in
base/version.go, so the sync_gateway binary inside each package holds its
"<version>-<build>" string and the commit SHA. This script downloads a few package
types, extracts the binary (it never runs it), and reports what each one contains.

    python3 -I check_packages.py --dir <download dir> 4.1.3 <full commit sha>

Exit status: 0 if every downloaded package has the commit, 1 if any package lacks it,
2 if no package could be downloaded (not published yet, or a private hotfix).
"""

import argparse
import io
import os
import re
import sys
import tarfile
import urllib.error
import urllib.request
import zipfile

BASE = "https://packages.couchbase.com/releases/couchbase-sync-gateway/{v}/couchbase-sync-gateway-{name}"
# Each package type is published on its own, so check more than one of them.
PACKAGES = [
    "enterprise_{v}_x86_64.deb",
    "enterprise_{v}_x86_64.zip",
    "community_{v}_x86_64.zip",
]


def binary_from_zip(data):
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        name = next(n for n in z.namelist() if n.endswith("/bin/sync_gateway"))
        return z.read(name)


def binary_from_deb(data):
    # A .deb is an ar archive: "!<arch>\n", then 60-byte member headers, each member
    # padded to an even length. The files are in the data.tar.* member.
    if not data.startswith(b"!<arch>\n"):
        raise ValueError("not an ar archive")
    pos = 8
    while pos < len(data):
        header = data[pos : pos + 60]
        name = header[:16].decode().strip().rstrip("/")
        size = int(header[48:58].decode().strip())
        member = data[pos + 60 : pos + 60 + size]
        if name.startswith("data.tar"):
            with tarfile.open(fileobj=io.BytesIO(member)) as tar:
                info = next(
                    m for m in tar.getmembers() if m.name.endswith("/bin/sync_gateway")
                )
                return tar.extractfile(info).read()
        pos += 60 + size + (size % 2)
    raise ValueError("no data.tar member")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--dir", required=True, help="empty directory to download the packages into"
    )
    ap.add_argument("version")
    ap.add_argument(
        "sha", help="full 40-character commit SHA expected in every package"
    )
    args = ap.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.sha):
        sys.exit("sha must be the full 40-character commit SHA")
    os.makedirs(args.dir, exist_ok=True)

    checked = failed = 0
    for name in (p.format(v=args.version) for p in PACKAGES):
        url = BASE.format(v=args.version, name=name)
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                data = resp.read()
        except urllib.error.HTTPError as e:
            print(f"{name}: not downloadable (HTTP {e.code})")
            continue
        with open(os.path.join(args.dir, name), "wb") as f:
            f.write(data)
        binary = (
            binary_from_deb(data) if name.endswith(".deb") else binary_from_zip(data)
        )

        builds = sorted(
            {
                m.decode()
                for m in re.findall(re.escape(args.version.encode()) + rb"-\d+", binary)
            }
        )
        ok = args.sha.encode() in binary
        checked += 1
        if ok:
            print(
                f"{name}: OK      build {', '.join(builds) or '?'}, has {args.sha[:9]}"
            )
        else:
            failed += 1
            # Not finding the expected SHA is the failure. The other 40-hex strings are
            # only a hint at which commit the package was built from.
            others = sorted(
                {
                    m.decode()
                    for m in re.findall(
                        rb"(?<![0-9a-f])[0-9a-f]{40}(?![0-9a-f])", binary
                    )
                }
            )
            print(
                f"{name}: MISSING build {', '.join(builds) or '?'}, no {args.sha[:9]}; 40-hex strings found: {', '.join(o[:9] for o in others) or 'none'}"
            )

    if checked == 0:
        sys.exit(2)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
