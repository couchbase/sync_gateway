#!/usr/bin/env python3
"""Print the Sync Gateway rows of the "Mobile Engineering Operations" Confluence page, with dates.

The page keeps its CC/RC/GA dates as ADF date nodes. The markdown form of the page drops
them, so fetch the page with contentFormat "adf" and pass the saved result to this script:

    python3 -I confluence_ga_dates.py <saved getConfluencePage result> [version ...]

The file can be the whole tool result ({"content": {"nodes": [{"body": ...}]}}), or the ADF
document itself. With versions, only rows that name one of them are printed.
"""

import datetime
import json
import re
import sys


def load_doc(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if "content" in data and isinstance(data["content"], dict):
        data = data["content"]["nodes"][0]["body"]
    if isinstance(data, str):
        data = json.loads(data)
    return data


def text(node):
    if isinstance(node, list):
        return " ".join(text(c) for c in node)
    if not isinstance(node, dict):
        return ""
    kind = node.get("type")
    attrs = node.get("attrs", {})
    if kind == "text":
        return node.get("text", "")
    if kind == "date":
        ts = int(attrs["timestamp"]) / 1000
        return "[" + datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%d") + "]"
    if kind == "status":
        return "[" + attrs.get("text", "") + "]"
    if kind in ("inlineCard", "blockCard"):
        return attrs.get("url", "")
    if kind == "mention":
        return attrs.get("text", "")
    return " ".join(text(c) for c in node.get("content", []))


def rows(node):
    if isinstance(node, dict):
        if node.get("type") == "tableRow":
            yield [re.sub(r"\s+", " ", text(c)).strip() for c in node.get("content", [])]
            return
        for child in node.get("content", []):
            yield from rows(child)


def main():
    doc = load_doc(sys.argv[1])
    versions = sys.argv[2:]
    # Minor releases are listed by x.y only ("SGW 4.1"), so x.y.0 also matches x.y.
    versions += [v[:-2] for v in versions if re.fullmatch(r"\d+\.\d+\.0", v)]
    for cells in rows(doc):
        line = " || ".join(cells)
        if not re.search(r"\bSGW\b|Sync Gateway", line):
            continue
        if versions and not any(re.search(r"(?<![\d.])" + re.escape(v) + r"(?![\d.])", line) for v in versions):
            continue
        # The in-progress table has a long "Update" column; the dates are in the first four cells.
        print(" || ".join(cells[:5])[:600])


if __name__ == "__main__":
    main()
