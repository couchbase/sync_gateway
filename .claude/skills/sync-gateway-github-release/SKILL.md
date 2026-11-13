---
name: sync-gateway-github-release
description: Use when creating, drafting, updating, or publishing GitHub releases for Sync Gateway release branches (release/x.y.z) - for requests like "make the GitHub releases for 3.3.9, 4.0.9 and 4.1.3", "draft the 4.1.3 release notes on GitHub", "refresh the draft release", or "publish the 4.1.3 release". Not for Jira release-note triage (that is sync-gateway-release-triage).
---

# Sync Gateway GitHub Releases

Each Sync Gateway release has a GitHub release in `couchbase/sync_gateway`. All recent ones have the same shape, so make new ones match exactly.

| Field | Value | Example |
|---|---|---|
| Tag | `x.y.z` (no `v`) | `4.1.3` |
| Target | `release/x.y.z` when drafting; the checked GA commit when publishing | `release/4.1.3` |
| Title | `x.y.z — Month YYYY` (one space, em dash `—` U+2014, one space). Month is the GA month, not today. | `4.1.3 — November 2026` |
| Body line 1 | The docs release-notes page for `x.y`, with `#<anchor>` of the version's heading when the page has one | `https://docs.couchbase.com/sync-gateway/4.1/product-notes/release-notes.html#maint-4-1-3` |
| Body rest | A blank line, then GitHub's generated notes, unchanged | `## What's Changed` ... `**Full Changelog**: .../compare/4.1.2...4.1.3` |

Releases are made in two stages: a **draft** some time before GA, then **publish** on GA day. A draft does not create the tag. GitHub creates a lightweight tag when the release is published.

The scripts in this skill do the read-only work. They are Python 3 with no extra packages. Run them from the `sync_gateway` checkout, and write their output to the scratchpad. `$SKILL_DIR` below is this skill's base directory.

## Rules

- **No GitHub writes without approval.** Show the preview for every release and wait for a yes before `gh release create`, `gh release edit`, or a publish. A yes to drafting is not a yes to publishing.
- **Never edit or delete an existing release without asking.** If a release or draft for the tag already exists, show the diff to what you would write, and ask.
- **Do not hand-edit the generated notes.** No reordering, no removed test-only PRs, no fixed PR titles. The only addition is the docs link at the top.
- **Never switch branches.** Other sessions share this checkout. Run `git fetch origin --tags` (never `--depth`) and read `origin/release/x.y.z`.
- **Never guess the GA month from today's date.** Look it up (step 2). If the sources disagree or none has it, ask.
- **A `STOP` line from a script means ask the user before going on** for that version. Show `CHECK` lines to the user, but they do not block.

## Workflow

### 1. Prepare every version

```sh
git fetch origin --tags
python3 -I $SKILL_DIR/release_prep.py --out <scratchpad> 3.3.9 4.0.9 4.1.3
```

For each version, the script:

- checks that `origin/release/x.y.z` exists, and that no release, draft, or tag exists for it yet
- finds the previous **published** release in the same `x.y` line, including 4-part hotfixes (`4.1.1` follows `4.1.0.1`), and checks that it is an ancestor of the branch
- reads the version's heading on the public docs release-notes page: the GA month, and the heading's anchor for the docs link. With no heading yet, the link is the top of the page.
- writes `release-x.y.z.md`: the docs link, a blank line, and GitHub's generated notes (the `generate-notes` API is read-only)
- compares the PR count with the commit count, and lists commits without a PR, PR titles tagged with another version, and a wrong `**Full Changelog**` line

An `x.y.0` has no earlier release in its line, so the script stops and you ask which tag to start from.

### 2. Find the GA month

Use these sources in order. The user's own answer always wins.

1. **Jira release epic (CM project).** Use the Atlassian tools with cloud ID `jira.issues.couchbase.com`:

   ```
   searchJiraIssuesUsingJql  jql: project = CM AND issuetype = Epic AND summary ~ "\"SGW\" \"Maintenance Release\"" AND summary ~ "\"4.1.3\""
                             fields: ["summary", "status", "customfield_10071"]
   ```

   `summary ~` is a fuzzy match, so keep only the epic whose summary has the exact version (for example CM-1827, "SGW Maintenance Release Ticket - 4.1.3"). The date is the **GA Estimated Date** field, `customfield_10071`. It is often empty until late in the release. Older epics have the date in the summary ("SGW - Maintenance Release - 4.1.2- Sept 18 2026").
2. **Public docs:** the `docs:` line from step 1. Only the month and year are taken from the heading.
3. **Confluence "Mobile Engineering Operations" page**, only if both sources above have no month. The page is edited by hand and its layout changes, so treat it as a hint. It is page `2406279808` on cloud ID `confluence.issues.couchbase.com`. Fetch it with `getConfluencePage` and `contentFormat: "adf"`, because the `markdown` format drops its dates. The result is saved to a file. Then:

   ```sh
   python3 -I $SKILL_DIR/confluence_ga_dates.py <saved result file> 3.3.9 4.0.9 4.1.3
   ```

   It prints each part of the page that names Sync Gateway and one of the versions, with dates as `[YYYY-MM-DD]`. Read the GA date from that text. If it prints nothing useful, skip this source.

Sources do sometimes disagree. For 3.3.0, Confluence has 2025-07-21, GitHub has July 2025, and the docs have August 2025.

### 3. Preview, then create drafts

Show one table for all versions: version, title, GA month from each source, target, branch head, previous release, PR count, and any `STOP` or `CHECK` lines. Then show each full body. After a yes:

```sh
gh release create "$V" --draft --target "release/$V" --title "$V — $MONTH" --notes-file <scratchpad>/release-$V.md
```

Do not pass `--generate-notes` or `--latest`. The body is already complete, and "latest" is set on publish. Give the user the draft URL from the output.

### 4. Publish (only when the user asks)

Publish happens on GA day.

1. `git fetch origin --tags`, and set `SHA` to the full commit of `origin/release/$V`.
2. **Check that the published packages were built from `SHA`:**

   ```sh
   python3 -I $SKILL_DIR/check_packages.py --dir <scratchpad>/packages-$V "$V" "$SHA"
   ```

   Release builds write `<version>-<build>` and the commit SHA into the `sync_gateway` binary. The script downloads a `.deb` and two `.zip` packages, reads the binary in each (it never runs it), and prints `OK` or `MISSING` per package.
   - **Exit 0 (all OK):** show the build number and continue.
   - **Exit 1 (a package is `MISSING`):** stop. Show the user each package's build and the commit it was built from, and ask before publishing.
   - **Exit 2 (nothing downloadable):** the packages are not published yet, or never are, as for most 4-part hotfixes. Ask the user to confirm that `SHA` is the GA commit.
3. Check the GA month again (step 2), because GA dates can slip into the next month. If the month changed, ask before `gh release edit "$V" --title "$V — $MONTH"`.
4. Run `release_prep.py` again for the version. The docs heading may exist now, so the link can gain its anchor. Ignore its `STOP` line about the existing draft. Compare the new body with the draft: `diff <(gh release view "$V" --json body | jq -j .body) <scratchpad>/release-$V.md`. Use `jq -j`, because `--jq .body` adds a newline and every body then looks changed. If they differ, show the diff and ask before `gh release edit "$V" --notes-file <scratchpad>/release-$V.md`.
5. Work out "latest": only the highest version across **all** published releases gets it (see `isLatest` in `gh release list`). A 3.3.x or 4.0.x release is never latest while 4.1.x exists.
6. Publish in rising version order, so the latest one is published last. Pass `SHA` as `--target`, not the branch, so a push to the branch during the publish cannot change what gets tagged:

   ```sh
   gh release edit "$V" --target "$SHA" --draft=false --latest=false   # every release except the highest
   gh release edit "$V" --target "$SHA" --draft=false --latest         # the highest version only
   ```

7. Check that `git ls-remote --tags origin "refs/tags/$V"` shows `SHA`, and that `gh release list --limit 5` shows the right Latest.

## Common mistakes

| Mistake | Fix |
|---|---|
| `-` or `–` instead of `—`, or two spaces (`4.0.7  — July 2026`) | Copy the title format exactly. |
| Month from today's date or the draft date | Look it up (step 2). Ask if the sources disagree. |
| `--generate-notes` with `--notes` | You cannot preview the exact body before it is written. Use the file from `release_prep.py`. |
| Publishing without `--latest=false` | GitHub may mark a 3.3.x or 4.0.x release as Latest. |
| Trusting the branch head as the GA build | Run `check_packages.py` (step 4, item 2). |
| Second `gh release create` for a tag that has a draft | Drafts can share a tag name, so you get a duplicate. Do not create a release when `release_prep.py` reports one. |
