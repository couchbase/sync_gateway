---
name: sync-gateway-release-triage
description: Use when preparing Jira for one or more Sync Gateway patch or minor releases - setting behavior-change labels, deciding which CBG tickets go public in the release notes (publicise-at-ga), finding test-only or flaky-test tickets, fixing issue types, or making ticket titles short enough for release notes. Use for requests like "triage the 4.1.3 tickets", "label the tickets for 3.3.9, 4.0.9 and 4.1.3", "which tickets in this filter are test-only", or a Jira filter link with fixVersions.
---

# Sync Gateway Release Triage

Before a release, every CBG ticket with that fixVersion needs:

- the right **issue type**: Bug, Improvement, or Task
- one **behavior label** (see [Behavior labels](#behavior-labels))
- the right **visibility**: the `publicise-at-ga` label puts a ticket in the public release notes
- a **short title**, because the title is the only text that goes into the release notes

Several releases often run at the same time (for example 3.3.9, 4.0.9 and 4.1.3). One fix then has one ticket per version. Decide once per fix and apply the same decision to every ticket of that fix.

## Rules

- **No Jira writes without approval.** Show a table with hyperlinks first, and wait for a yes. Interactive answers from the user count as approval for those tickets only.
- **Labels are American spelling only**: `behavior`, not `behaviour`. The user may type British spelling. Always write the American label.
- **`labels` in `editJiraIssue` replaces the whole list.** Fetch the current labels first, and send the full list: keep every label you are not changing, replace any old behavior label with the new one, and leave out only the labels the user approved removing. Otherwise you delete labels like `ISGR`, `backport-candidate`, or `goroutine-leak`, or you leave two behavior labels on one ticket.
- **All CBG tickets have the security level Private.** Do not change it. A ticket is public only through the `publicise-at-ga` label.
- **Never switch branches.** Other sessions share this checkout. Read release code with `git log origin/release/x.y.z` and `git show <sha>`. Run `git fetch origin` first. Never use `git fetch --depth`.
- **Count from the query result, not from memory.** Re-run the JQL and count the rows before you state a number. Follow `nextPageToken` until there are no more pages.
- **Use the exact filter ID the user gives.** If you work from a filter, read its JQL first.

Use cloud ID `jira.issues.couchbase.com`. Issue type IDs: Task `10002`, Bug `10004`, Improvement `10102`.

## Behavior labels

| Label | Use when |
|---|---|
| `no-behavior-change` | Only tests, test helpers, CI, or docs change. Nothing ships to users. |
| `ok-behavior-change` | A normal fix. Behavior goes from wrong to right in the way a user expects, for example a crash, hang, leak, or race that no longer happens. |
| `acceptable-behavior-change` | A user can notice a change that is more than the fix itself: stricter checks (for example disabled users now lose their sessions), a different wire format, new file names or output, or a replication that now skips something it used to retry. |
| `negative-behavior-change` | The change can break a working setup. Call this out to the user. |

## Workflow

### 1. Get the ticket list

Ask for the versions if the user did not give them. Then query:

```
project = CBG AND fixVersion IN (<versions>) AND (resolution IS EMPTY OR resolution NOT IN ("Won't Do", "Won't Fix", Duplicate, "Not a Bug")) ORDER BY fixVersion, key
```

Request `["summary", "issuetype", "labels", "status", "resolution", "fixVersions"]`. Group the tickets by fix: backports of one fix share an original ticket and most of the title, and they differ only in the `[x.y.z backport]` prefix. The prefix can be `backport` or `Backport`, so match it in either case.

### 1b. Find commits with no matching ticket

Compare the release branches against Jira, in both directions. For each version, list the commits since the previous release tag:

```bash
git log --no-merges --format='%h%x09%an%x09%s' <previous tag>..origin/release/<version>
```

Collect every `CBG-` key from those commits and fetch the keys with `key in (...)`. Large results go to a file, so read them with `jq`. Report:

- **Commits with no key.** Look at each diff. A Go version bump or a dependency bump for vulnerabilities ships to users and needs a ticket. Repo-only changes (CODEOWNERS, CI) and code that only runs with the `cb_sg_devmode` build tag do not.
- **Commits whose ticket does not have that fixVersion.** Common causes: the commit uses the original ticket instead of a backport ticket; one backport ticket is used on two branches; or the ticket names an older version that was already released without that commit.
- **Tickets with the fixVersion but no commit on the branch.** These are fine if they are resolved as Won't Do, Duplicate, or Not a Bug, or if they are release admin tasks.

Do not fix anything here. Show the gaps and suggest a fix for each one. Use the sync-gateway-backport-tickets skill to create any missing backport tickets.

### 2. Find each fix's code change

For each ticket, find the commit:

```bash
git log origin/release/<version> --format='%h %s' --grep='CBG-1234'
gh pr list --repo couchbase/sync_gateway --state merged --search 'CBG-1234' --json number,title,baseRefName
```

Also search with the key of the original ticket, because a backport commit can name either one. If there is no commit, report the ticket as "no code found" and do not guess.

With many tickets, give the diff reading to subagents: one per group of fixes, each returning a short summary per fix. The diffs then stay out of the main context.

### 3. Test-only tickets

A change is test-only when every changed file is one of these: a `*_test.go` file, `testdata/`, a test helper (`rest/utilities_testing*.go`, `base/util_testing.go`, `db/util_testing.go`), `.ci/`, `Jenkinsfile`, or `integration-test/`. A flaky-test fix is test-only only when its diff is test-only. A flaky-test fix that changes product code is a product change, so handle it in step 4.

Show the test-only tickets in one table and ask for approval. Then, for each one:

- type → Task
- add `no-behavior-change`
- remove `publicise-at-ga`

### 4. Product changes: decide interactively, one fix at a time

For each fix, use AskUserQuestion. The user may see only the dialog, so put the context into the question text: the ticket keys, what the bug was, what the fix changes, and what a user would notice. Ask these questions in one call:

1. **Visibility**: Public (keep or add `publicise-at-ga`) or Private (remove it). Recommend Private for fixes that only matter to Capella or to tests. If the user chooses Private for a real bug fix, draft a Jira comment that gives the reason. The comment is written in step 6, after the user sees its text.
2. **Behavior**: the four labels, with your recommendation first and a reason in its description.
3. **Title**: your suggested title, keep the current title, or flag it for a rename later.
4. **Type**: ask only when the current type looks wrong, for example an Improvement that is a bug fix.

Record each answer. Do not write to Jira between questions, so the user can still change earlier answers.

### 5. Titles for the release notes

- Describe the symptom that a user sees, not the internals of the fix. Good: "Document writes can hang forever when a database closes". Bad: "Add terminator select to reserveNotify send".
- Keep it short: about 80 characters after the prefix.
- Use plain words. Names of APIs and options are fine, for example `PUT /{db}/_config/audit` or `sgcollect_info`.
- Keep the existing prefix exactly, including its case (`[4.1.3 Backport]` or `[4.1.3 backport]`). Do not add a ` - ` after it.
- Use the same text after the prefix for every ticket of one fix.

### 6. Apply

Show one table of every change: ticket, labels added and removed, new title, new type, and the text of any Jira comment. Wait for a yes. Then fetch the current labels and summaries of all target tickets in one query, and send all `editJiraIssue` and `addCommentToJiraIssue` calls in parallel. Each `editJiraIssue` call has the full `labels` list, plus `summary` and `issuetype` when they change.

### 7. Check

Re-run the query for tickets that still need a behavior label:

```
project = CBG AND fixVersion IN (<versions>) AND (labels IS EMPTY OR labels NOT IN (no-behavior-change, ok-behavior-change, acceptable-behavior-change, negative-behavior-change)) AND (resolution IS EMPTY OR resolution NOT IN ("Won't Do", "Won't Fix", Duplicate, "Not a Bug"))
```

Report what is left and why. List release admin Tasks (for example the release ticket itself) apart from the rest, so the user can decide if they need a label. Then give a short summary table: ticket, type, behavior label, public or private, and the new title.

## Common mistakes

| Mistake | Fix |
|---|---|
| A label list sent without the existing labels | Fetch first, and send the full list |
| `behaviour` labels | Always American spelling |
| A count given from memory | Re-run the query and count the rows |
| A flaky-test fix marked test-only when it also changed product code | Check every file in the diff |
| A prerequisite backport treated as test-only because a test needed it | Judge it by its own diff. It often fixes a real bug, like a goroutine leak. |
| A dependency or Go toolchain bump marked `acceptable-behavior-change` | Label it by the fix it was made for, usually `ok-behavior-change`, even for a Go minor version jump. Mention the size of the version jump in the question, so the user can choose otherwise. |
| A description change passed as a top-level `description` argument to `editJiraIssue` | Jira ignores it without an error. Put `description` inside `fields`, and pass `contentFormat: "markdown"`. |
| Different titles for backports of the same fix | Use one text after the prefix |
