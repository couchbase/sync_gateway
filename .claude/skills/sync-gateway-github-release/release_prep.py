#!/usr/bin/env python3
# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

"""Read-only preparation for Sync Gateway GitHub releases (skill steps 1-3).

For each version it checks the release branch, existing releases and tags, finds the
previous published release in the same x.y line, reads the version's heading on the
public docs release-notes page (GA month and anchor), and builds the release body from
GitHub's generated notes. Nothing is written to GitHub.

    python3 -I release_prep.py --out <dir> 3.3.9 4.0.9 4.1.3

Run it from the sync_gateway checkout after `git fetch origin --tags`. Needs `git` and
an authenticated `gh`. Each body is written to <dir>/release-<version>.md.

Lines starting with "STOP" need the user before going on; "CHECK" lines should be shown
to the user but do not block.
"""

import argparse
import html.parser
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request

REPO = "couchbase/sync_gateway"
DOCS_URL = (
    "https://docs.couchbase.com/sync-gateway/{xy}/product-notes/release-notes.html"
)


def run(*cmd):
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def try_run(*cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else None


def version_key(v):
    return tuple(int(p) for p in v.split("."))


def is_version(s):
    return re.fullmatch(r"\d+(\.\d+){2,3}", s) is not None


def all_releases():
    """Every GitHub release, drafts included (drafts are only visible with push access)."""
    out = run(
        "gh",
        "api",
        "--paginate",
        f"repos/{REPO}/releases",
        "--jq",
        ".[] | {tag: .tag_name, draft: .draft, name: .name}",
    )
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def previous_release(version, releases):
    """The highest published release in the same x.y line that is lower than version."""
    xy = version.split(".")[:2]
    older = [
        r["tag"]
        for r in releases
        if not r["draft"]
        and is_version(r["tag"])
        and r["tag"].split(".")[:2] == xy
        and version_key(r["tag"]) < version_key(version)
    ]
    return max(older, key=version_key, default=None)


class Headings(html.parser.HTMLParser):
    """Collects (id, text) for every h1-h6 on the page."""

    def __init__(self):
        super().__init__()
        self.headings = []
        self._open = None

    def handle_starttag(self, tag, attrs):
        if re.fullmatch(r"h[1-6]", tag):
            self._open = [dict(attrs).get("id"), ""]

    def handle_data(self, data):
        if self._open is not None:
            self._open[1] += data

    def handle_endtag(self, tag):
        if self._open is not None and re.fullmatch(r"h[1-6]", tag):
            self.headings.append(tuple(self._open))
            self._open = None


def docs_heading(version):
    """Return (url, anchor, "Month YYYY") for the version's heading, or Nones if absent."""
    xy = ".".join(version.split(".")[:2])
    url = DOCS_URL.format(xy=xy)
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            page = resp.read().decode("utf-8")
    except urllib.error.URLError as e:
        return url, None, None, f"could not fetch the docs page: {e}"
    parser = Headings()
    parser.feed(page)
    # Headings look like "4.1.3 — November 2026", with thin spaces around the dash.
    pattern = re.compile(
        r"^" + re.escape(version) + r"\s*[—–-]\s*([A-Z][a-z]+ \d{4})\b"
    )
    for anchor, text in parser.headings:
        m = pattern.match(" ".join(text.split()))
        if m:
            return url, anchor, m.group(1), None
    return url, None, None, None


def prepare(version, releases, out_dir):
    print(f"== {version}")
    problems = []

    branch = f"origin/release/{version}"
    head = try_run("git", "rev-parse", "--verify", "--quiet", f"{branch}^{{commit}}")
    if not head:
        print(f"STOP: {branch} does not exist (run `git fetch origin`?)")
        return
    head = head.strip()
    print(
        f"branch:   {branch} at {head[:9]} {run('git', 'log', '-1', '--format=%cs %s', head).strip()}"
    )

    existing = [r for r in releases if r["tag"] == version]
    for r in existing:
        problems.append(
            f"STOP: a {'draft' if r['draft'] else 'published'} release already exists: {r['name']}"
        )
    tag = run("git", "ls-remote", "--tags", "origin", f"refs/tags/{version}").split()
    if tag and tag[0] != head:
        problems.append(
            f"STOP: tag {version} exists at {tag[0][:9]}, not at the branch head"
        )

    prev = previous_release(version, releases)
    if prev is None:
        problems.append(
            "STOP: no earlier published release in this line; ask which tag to start from"
        )
    elif (
        subprocess.run(["git", "merge-base", "--is-ancestor", prev, head]).returncode
        != 0
    ):
        problems.append(f"STOP: previous release {prev} is not an ancestor of {branch}")
    print(f"previous: {prev}")

    url, anchor, month, err = docs_heading(version)
    if err:
        problems.append(f"CHECK: {err}")
    link = f"{url}#{anchor}" if anchor else url
    print(f"docs:     {month or 'no heading for this version yet'}")
    print(f"link:     {link}")

    for p in problems:
        print(p)
    if prev is None or any(p.startswith("STOP: previous") for p in problems):
        return

    notes = run(
        "gh",
        "api",
        f"repos/{REPO}/releases/generate-notes",
        "-f",
        f"tag_name={version}",
        "-f",
        f"target_commitish=release/{version}",
        "-f",
        f"previous_tag_name={prev}",
        "--jq",
        ".body",
    )
    body = f"{link}\n\n{notes}"
    path = f"{out_dir}/release-{version}.md"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)

    prs = [line for line in notes.splitlines() if line.startswith("* ")]
    commits = run("git", "log", "--format=%h %s", f"{prev}..{head}").splitlines()
    print(f"PRs:      {len(prs)} in the notes, {len(commits)} commits on the branch")
    if not commits:
        print(f"STOP: no commits on {branch} since {prev}")
    for c in commits:
        if not re.search(r"\(#\d+\)$", c):
            print(f"CHECK: commit without a PR number: {c}")

    tags = {}
    for line in prs:
        for t in re.findall(r"\[(\d+\.\d+\.\d+(?:\.\d+)?)[^\]]*\]", line):
            tags[t] = tags.get(t, 0) + 1
    for t, n in sorted(tags.items()):
        if t != version:
            print(f"CHECK: {n} PR title(s) tagged [{t}...]")

    expected = (
        f"**Full Changelog**: https://github.com/{REPO}/compare/{prev}...{version}"
    )
    if notes.rstrip().splitlines()[-1] != expected:
        print(f"CHECK: last line is not `{expected}`")
    print(f"body:     {path}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--out", required=True, help="directory for the release-<version>.md bodies"
    )
    ap.add_argument("versions", nargs="+")
    args = ap.parse_args()
    bad = [v for v in args.versions if not is_version(v)]
    if bad:
        sys.exit(f"not a version: {' '.join(bad)}")
    remote = try_run("git", "remote", "get-url", "origin") or ""
    if "couchbase/sync_gateway" not in remote:
        sys.exit(
            "run this from a couchbase/sync_gateway checkout (origin must be couchbase/sync_gateway)"
        )
    releases = all_releases()
    for v in sorted(args.versions, key=version_key):
        prepare(v, releases, args.out)


if __name__ == "__main__":
    main()
