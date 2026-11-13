#!/usr/bin/env python3
# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

"""Print the parts of the "Mobile Engineering Operations" Confluence page that mention
the given Sync Gateway versions, with their dates.

The page is edited by hand and has no fixed structure, so this does not look for
particular columns or labels. It turns the page into text blocks (table rows,
paragraphs, list items, headings), keeps the blocks that name Sync Gateway and one of
the versions, and prints them for a person (or Claude) to read.

The page keeps its dates as ADF date nodes, and the markdown form of the page drops
them, so fetch the page with contentFormat "adf" and pass the saved result:

    python3 -I confluence_ga_dates.py <saved getConfluencePage result> 3.3.9 4.0.9 4.1.3

The file can be the whole tool result ({"content": {"nodes": [{"body": ...}]}}) or the
ADF document itself. Exit status 3 means nothing matched.
"""

import datetime
import json
import re
import sys

BLOCKS = {"tableRow", "paragraph", "listItem", "heading"}
PRODUCT = re.compile(r"\bSGW\b|Sync Gateway", re.I)


def load_doc(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data.get("content"), dict):
        data = data["content"]["nodes"][0]["body"]
    if isinstance(data, str):
        data = json.loads(data)
    return data


def text(node):
    if isinstance(node, list):
        return " ".join(text(c) for c in node)
    if not isinstance(node, dict):
        return ""
    kind, attrs = node.get("type"), node.get("attrs", {})
    if kind == "text":
        return node.get("text", "")
    if kind == "date":
        ts = int(attrs["timestamp"]) / 1000
        return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime(
            "[%Y-%m-%d]"
        )
    if kind == "status":
        return "[" + attrs.get("text", "") + "]"
    if kind in ("inlineCard", "blockCard"):
        return attrs.get("url", "")
    if kind == "mention":
        return attrs.get("text", "")
    sep = " | " if kind == "tableRow" else " "
    return sep.join(text(c) for c in node.get("content", []))


def blocks(node):
    """Yield the text of each outermost block, so a table row is not repeated per cell."""
    if isinstance(node, dict):
        if node.get("type") in BLOCKS:
            yield re.sub(r"\s+", " ", text(node)).strip()
            return
        for child in node.get("content", []):
            yield from blocks(child)


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    doc = load_doc(sys.argv[1])
    versions = sys.argv[2:]
    # Minor releases are often written as x.y ("SGW 4.1"), so x.y.0 also matches x.y.
    versions += [v[:-2] for v in versions if re.fullmatch(r"\d+\.\d+\.0", v)]
    wanted = [re.compile(r"(?<![\d.])" + re.escape(v) + r"(?![\d.])") for v in versions]
    found = 0
    for block in blocks(doc):
        if PRODUCT.search(block) and any(w.search(block) for w in wanted):
            print(block[:500])
            found += 1
    if not found:
        print(
            "nothing on the page names Sync Gateway and these versions; skip this source"
        )
        sys.exit(3)


if __name__ == "__main__":
    main()
