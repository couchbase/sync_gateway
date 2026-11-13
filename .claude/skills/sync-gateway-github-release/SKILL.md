---
name: sync-gateway-github-release
description: Use when creating, drafting, updating, or publishing GitHub releases for Sync Gateway release branches (release/x.y.z) - for requests like "make the GitHub releases for 3.3.9, 4.0.9 and 4.1.3", "draft the 4.1.3 release notes on GitHub", "refresh the draft release", or "publish the 4.1.3 release". Not for Jira release-note triage (that is sync-gateway-release-triage).
---

# Sync Gateway GitHub Releases

Each Sync Gateway release has a GitHub release in `couchbase/sync_gateway`. All recent ones have the same shape, so make new ones match exactly.

| Field | Value | Example |
|---|---|---|
| Tag | `x.y.z` (no `v`) | `4.1.3` |
| Target | `release/x.y.z` | `release/4.1.3` |
| Title | `x.y.z — Month YYYY` (one space, em dash `—` U+2014, one space). Month is the GA month, not today. | `4.1.3 — November 2026` |
| Body line 1 | `https://docs.couchbase.com/sync-gateway/<x.y>/product-notes/release-notes.html`, plus `#<anchor>` of the version's heading when the page has one (step 3) | `.../sync-gateway/4.1/product-notes/release-notes.html#maint-4-1-3` |
| Body rest | A blank line, then GitHub's generated notes, unchanged | `## What's Changed` ... `**Full Changelog**: .../compare/4.1.2...4.1.3` |

Releases are made in two stages, like before: a **draft** some time before GA, then **publish** on GA day. A draft does not create the tag. GitHub creates a lightweight tag at the head of `release/x.y.z` when the release is published.

## Rules

- **No GitHub writes without approval.** Show the preview (title, target, previous tag, PR count, full body) for every release and wait for a yes before `gh release create`, `gh release edit`, or a publish. A yes to drafting is not a yes to publishing.
- **Never edit or delete an existing release without asking.** If a release or draft for the tag already exists, show the diff to what you would write, and ask.
- **Do not hand-edit the generated notes.** No reordering, no removed test-only PRs, no fixed PR titles. The only addition is the docs link at the top.
- **Never switch branches.** Other sessions share this checkout. Run `git fetch origin --tags` (never `--depth`) and read `origin/release/x.y.z`.
- **Look up the GA month (step 2b). Never guess it from today's date.** If the sources disagree or none has it, ask.

## Workflow

Do steps 1–3 for every version first, then show one preview for all of them.

### 1. Check the version

```sh
V=4.1.3; XY=$(echo $V | cut -d. -f1-2)
git fetch origin --tags
git rev-parse --verify origin/release/$V          # the branch must exist
gh release list --limit 200 --json tagName,isDraft,isLatest --jq ".[] | select(.tagName==\"$V\")"   # must be empty (lists drafts too)
git ls-remote --tags origin "refs/tags/$V"        # should be empty
```

- A release or draft already exists: stop for that version. Show what is there, and ask.
- The tag already exists: GitHub ignores `--target` and uses the tag. Check that it points at the head of `origin/release/$V`. If not, stop and ask.

### 2. Find the previous release

The start point is the previous **published** release in the same `x.y` line, including 4-part hotfixes (`4.1.1` follows `4.1.0.1`).

```sh
PREV=$({ gh release list --limit 200 --exclude-drafts --json tagName --jq '.[].tagName' | grep -E "^${XY//./\\.}\.[0-9]+(\.[0-9]+)?$"; echo "$V"; } | sort -uV | grep -B1 -x "$V" | head -1)
git merge-base --is-ancestor "$PREV" "origin/release/$V" && echo ok
```

- If `PREV` equals `V`, there is no earlier release in the line (an `x.y.0`). Stop and ask the user which tag or commit to start from.
- If `PREV` is not an ancestor of the release branch, stop and ask. The notes would be wrong.

### 2b. Find the GA month

Check all three sources. The user's own answer always wins.

**1. Jira release epic (CM project).** Use the Atlassian tools with cloud ID `jira.issues.couchbase.com`:

```
searchJiraIssuesUsingJql  jql: project = CM AND issuetype = Epic AND summary ~ "\"SGW\" \"Maintenance Release\"" AND summary ~ "\"4.1.3\""
                          fields: ["summary", "status", "customfield_10071"]
```

`summary ~` is a fuzzy match, so keep only the epic whose summary has the exact version. Example: CM-1827 is "SGW Maintenance Release Ticket - 4.1.3". The GA date is the **GA Estimated Date** field, `customfield_10071` (for example `2026-09-18` on CM-1716). Older epics also have the date in the summary ("SGW - Maintenance Release - 4.1.2- Sept 18 2026"). The field is often empty until late in the release.

**2. Confluence "Mobile Engineering Operations" page** (page `2406279808`, cloud ID `confluence.issues.couchbase.com`). The current releases table has `GA: Target <date>`, and the "Initiatives Completed" table has a GA Date column. The dates are ADF date nodes. **The `markdown` format drops them**, so call `getConfluencePage` with `contentFormat: "adf"`. The result is too big to show, so it is saved to a file. Read it with the helper in this skill:

```sh
python3 -I .claude/skills/sync-gateway-github-release/confluence_ga_dates.py <saved result file> 3.3.9 4.0.9 4.1.3
# IN PROGRESS || SGW 3.3.9/ 4.0.9/ 4.1.3 || ... || CC: [2026-11-04] RC: [2026-11-06] Docs : [2026-11-06] GA: Target [2026-11-13] || TBD
```

**3. Public docs release notes.** The docs team adds each version's heading before GA. Save the page; step 3 uses it too:

```sh
DOCS_URL="https://docs.couchbase.com/sync-gateway/$XY/product-notes/release-notes.html"
DOCS=<scratchpad>/docs-$XY.html
curl -fsSL -o "$DOCS" "$DOCS_URL"
grep -oE "(>|^)${V//./\\.}&#8201;&#8212;&#8201;[A-Z][a-z]+ [0-9]{4}" "$DOCS" | sed -E 's/.*&#8201;//' | head -1
# November 2026
```

Take only the month and year from the heading. It uses thin spaces (`&#8201;`), not the plain spaces of the GitHub title. 4-part hotfixes (`4.1.0.1`) usually have no heading.

Show the month from each source in the preview. If they disagree, ask. They do sometimes disagree: for 3.3.0, Confluence has 2025-07-21, GitHub has July 2025, and the docs have August 2025.

### 3. Build the body

First find the anchor of the version's heading in the saved docs page (step 2b). The id is read from the heading itself, so a found anchor always exists on the page. Do not build it from the version number: older headings have ids like `4-0-6june-2026`, not `maint-4-0-6`.

```sh
ANCHOR=$(grep -oE "<h[1-6] id=\"[^\"]+\"><a class=\"anchor\" href=\"#[^\"]+\"></a>${V//./\\.}&#8201;" "$DOCS" \
  | sed -E 's/^<h[1-6] id="([^"]+)".*/\1/' | head -1)
DOCS_LINK="${DOCS_URL}${ANCHOR:+#$ANCHOR}"
# https://docs.couchbase.com/sync-gateway/4.1/product-notes/release-notes.html#maint-4-1-3
```

If `ANCHOR` is empty (no heading for this version yet, as for most 4-part hotfixes), the link is the top of the page. Tell the user which one you used.

```sh
OUT=<scratchpad>/release-$V.md
{ printf '%s\n\n' "$DOCS_LINK"
  gh api repos/couchbase/sync_gateway/releases/generate-notes \
    -f tag_name="$V" -f target_commitish="release/$V" -f previous_tag_name="$PREV" --jq .body
} > "$OUT"
grep -c '^\* ' "$OUT"                                  # PRs in the notes
git rev-list --count "$PREV..origin/release/$V"        # commits on the branch
```

The `generate-notes` call is read-only. It creates nothing.

Sanity checks:
- The two counts are usually equal (every commit on a release branch is a squash-merged PR). If they differ, list the commits with no PR (`git log --oneline $PREV..origin/release/$V | grep -v '(#'`) and tell the user.
- Look for PR titles tagged with a different version (for example `[4.0.9 Backport]` in the 4.1.3 notes): `grep '^\* ' "$OUT" | grep -oE '\[[0-9][^]]*\]' | sort | uniq -c`. Only look at the `[...]` tags, because titles also name dependency versions (`update go to 1.26.10`). Point out other versions, but do not change them.
- The last line must be `**Full Changelog**: https://github.com/couchbase/sync_gateway/compare/$PREV...$V`.

### 4. Preview, then create drafts

Show a table (version, title, GA month from Jira / Confluence / docs, target, previous tag, PR count) and each full body. After a yes:

```sh
gh release create "$V" --draft --target "release/$V" --title "$V — $MONTH" --notes-file "$OUT"
```

Do not pass `--generate-notes` or `--latest`. The body is already complete, and "latest" is set on publish. Give the user the draft URL from the output.

### 5. Publish (only when the user asks)

Publish happens on GA day. The tag is made from the branch head at this moment, so the branch must hold exactly what shipped.

1. `git fetch origin --tags`. Show the head commit of `origin/release/$V` and ask the user to confirm it is the GA build.
2. Check the GA month again (step 2b), because GA dates can slip into the next month. If the month changed, ask before `gh release edit "$V" --title "$V — $MONTH"`.
3. Fetch the docs page again and build the body again (step 3). The docs heading may exist now, so the link can gain its anchor. If the body differs from the draft (`diff <(gh release view "$V" --json body | jq -j .body) "$OUT"`; use `jq -j`, because `--jq .body` adds a newline and every body then looks changed), show the diff and ask before `gh release edit "$V" --notes-file "$OUT"`.
4. Work out "latest": only the highest version across **all** published releases gets it (see the `isLatest` row in `gh release list`). A 3.3.x or 4.0.x release is never latest while 4.1.x exists.
5. Publish in rising version order, so the latest one is published last. Pass the full SHA the user confirmed in item 1 as `--target`, not the branch, so a push to the branch during the publish cannot change what gets tagged:

```sh
gh release edit "$V" --target "$SHA" --draft=false --latest=false   # every release except the highest
gh release edit "$V" --target "$SHA" --draft=false --latest         # the highest version only
```

6. Check: `git ls-remote --tags origin "refs/tags/$V"` matches the branch head, and `gh release list --limit 5` shows the right Latest.

## Common mistakes

| Mistake | Fix |
|---|---|
| `-` or `–` instead of `—`, or two spaces (`4.0.7  — July 2026`) | Copy the title format exactly. |
| Month from today's date or the draft date | Look it up in Jira, Confluence and the docs (step 2b). Ask if they disagree. |
| `--generate-notes` with `--notes` | You cannot preview the exact body before it is written. Build the file in step 3. |
| Previous tag from `git tag` only | A tag can exist with no published release. Use the release list. |
| Publishing without `--latest=false` | GitHub may mark a 3.3.x or 4.0.x release as Latest. |
| Second `gh release create` for a tag that has a draft | Drafts can share a tag name, so you get a duplicate. Always do step 1. |
