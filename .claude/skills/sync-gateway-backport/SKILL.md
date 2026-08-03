---
name: sync-gateway-backport
description: Use when backporting merged Sync Gateway changes to a release branch - cherry-picking an upstream fix onto release/x.y.z, working through a set of backport tickets, or when a backport needs a prerequisite change that is not on the release branch yet.
---

# Sync Gateway Backport

Backport a merged `main` change to a `release/x.y.z` branch: one ticket, or a whole sprint's worth. Each backport is one commit, one branch, one PR. When backports depend on each other, they become a GitHub Stacked PR chain.

## Find the work

Backport tickets are clones of the original ticket, with `[x.y.z Backport]` in the summary and the release in `fixVersions`.

Ask Jira for the current user's open-sprint backports:

```
project = CBG AND assignee = currentUser() AND sprint IN openSprints()
  AND summary ~ "Backport" ORDER BY key ASC
```

Request only `["summary", "status"]` — the response still usually overflows to a file. Read the list from it rather than re-querying:

```bash
jq -r '.issues.nodes[] | "\(.key)\t\(.fields.status.name)\t\(.fields.summary)"' <saved-result-file>
```

Skip tickets whose *backport* ticket is already `In Review`/`Resolved` unless asked otherwise — check for an open PR before assuming. Work only the tickets the user named, or if they named none, the `Open` ones; confirm the list before starting a multi-ticket run.

## Per ticket

1. **Resolve the upstream change.** Fetch the backport ticket with `fields: ["summary","fixVersions","issuelinks"]`. The `Cloners` link points at the original ticket. Fetch that ticket's `status` too — it, not the backport ticket's status, decides whether the work is possible. Then:
   ```bash
   git log --oneline origin/main --grep='CBG-<original>'   # commit + (#PR) in the subject
   ```
   Release version comes from `fixVersions` / the summary prefix.

   **No hit means the change is not merged yet — do not improvise.** Sprint-scoped backport tickets are created alongside the original, so an `Open` backport whose original is still `In Review` is the normal early state, and the discovery JQL surfaces these routinely.
   ```bash
   gh pr list --search "CBG-<original>" --state all --json number,state,reviewDecision,mergedAt
   ```
   | Upstream state | Action |
   |---|---|
   | Merged | Proceed with the merge/squash SHA |
   | PR open | **Stop.** Report the PR, its review state, and that the backport is blocked. Do not create a submittable branch — the diff will still change |
   | No PR at all | Stop and report; confirm you have the right original ticket |

   When a PR is open and you want to pre-flight it, a throwaway trial is cheap and tells you what the real backport will cost — fetch `refs/pull/N/head`, cherry-pick the range from its merge-base, and record the conflicts and compile fixes. Mark the branch clearly as a trial, never submit it, and redo the work from the real squash commit once it lands.
2. **Fetch first, always.** A prerequisite may have landed since your last fetch — this is the single biggest waste of effort in a backport.
   ```bash
   git fetch origin --prune
   git fetch origin release/x.y.z:release/x.y.z   # keep the local trunk ref current
   ```
3. **Branch and cherry-pick.** Branch name = backport ticket key. Use `-c rerere.enabled=false` so you see the real conflicts instead of a replay of an earlier resolution.
   ```bash
   git checkout -b CBG-<backport> origin/release/x.y.z
   git -c rerere.enabled=false cherry-pick -x <upstream-sha>
   ```
4. **Resolve.** Before hand-merging anything, check whether the conflict exists only because a prerequisite is missing — see [Prerequisites](#prerequisites). Prefer the upstream side; keep the release branch's side only for pre-existing API differences.
5. **Commit** with the message convention (`git commit --amend` after `cherry-pick --continue`, which otherwise keeps the upstream subject):
   ```
   [x.y.z Backport] CBG-<backport>: <upstream subject, ticket prefix stripped>

   Cherry-picked from <sha> (CBG-<original>, #<PR>).
   <what was adapted and why, if anything>

   Co-authored-by: <upstream author>
   ```
6. **Verify** — every backport, no exceptions. Build, vet **and running the affected tests** all carry equal weight: adaptations routinely compile and vet clean, then fail at runtime.
   ```bash
   gofmt -l . && go build ./... && go vet ./...
   go test -count=1 -run '<affected tests>' ./<pkg>/          # every package the commit touched
   golangci-lint run --new-from-rev=origin/release/x.y.z ./<pkg>/...
   ```
   `--new-from-rev` is what makes lint usable — it reports only what this backport adds, instead of the base branch's hundreds of pre-existing findings. Lint every touched package, not just one.

   A new finding that exists identically on `main`, and is only "new" because the file is new to the release branch: fix it and list it as a deviation. A red CI is worse than an extra bullet in the body.

   Integration tests need `SG_TEST_BACKING_STORE=Couchbase`. If you can't run them, say so explicitly rather than implying they passed.

## Prerequisites

A conflict often means the release branch is missing a change the backport builds on. Find it before resolving by hand:

```bash
git log --oneline origin/main -L :<conflicted-function>:<file>   # what touched this before
git log --oneline origin/release/x.y.z -i --grep='CBG-<candidate>'  # is it on the branch?
```

| Prerequisite state | Action |
|---|---|
| Has its own backport ticket | Backport it as its own layer, stacked below |
| No backport ticket, low risk — test-only, or a small self-contained code change | Backport it anyway as its own layer; title it with the **upstream** ticket key; tell the user it has no backport ticket and offer to create the clone |
| No backport ticket, higher risk — behaviour change, wide blast radius, or you can't tell | **Stop and ask** (AskUserQuestion): backport the prerequisite / adapt the change to the release branch without it / skip the backport |

Adapting around a missing prerequisite is legitimate but costs more and diverges from upstream: every adaptation is a place the branches can drift. If you adapt, list each deviation in the PR body. **Re-run the cherry-pick once the prerequisite is in place** — conflicts frequently vanish entirely and the diff becomes upstream-identical, which is worth far more than a clever manual merge.

## Stacking dependent backports

Bottom of the stack = the prerequisite; top = the change that needs it.

```bash
git config rerere.enabled true
git config remote.pushDefault origin
gh stack init --base release/x.y.z CBG-<lower> CBG-<upper>
gh stack view --json          # never bare `gh stack view` - it opens a TUI
gh stack submit --auto --open # pushes, creates non-draft PRs, links the stack
```

`gh stack submit` sets each PR's base to the branch below it. Update the bodies afterwards (`gh pr edit N --body-file`) — `--auto` generates them from commit messages.

When a branch name is already taken (usually the upstream PR used the same ticket key), suffix the release: `CBG-5414-4.1.2`.

## PR title and body

Title — the ticket is the **backport** ticket, the subject is the **upstream PR title with its own ticket prefix stripped**:

```
[4.1.2 Backport] CBG-5591: stop indexes being built on default when not required
```

Body — exactly this shape, nothing else (no repo PR template):

```
CBG-5591

Clean cherry pick of #8495 to 4.1.2
```

For anything that did not apply cleanly:

```
CBG-5591

Unclean cherry pick of #8495 to 4.1.2
Changes from main commit:
- `rest/manualbucketpooltest/database_init_manager_test.go` — package doesn't exist on 4.1.2; the signature change was applied to the copy of that test in `rest/` instead
- `rest/database_init_manager_test.go` — `dbConfig.setup()` takes an extra `forcePerBucketAuth` arg on 4.1.2
```

One bullet per deviation, naming the file and the reason. "Clean" means the cherry-pick applied with no conflicts *and* you changed nothing afterwards — a compile fix still makes it unclean. For a stacked PR, add a line naming the PR it sits on.

## Red flags

- Building a backport branch when the upstream PR hasn't merged — check the *original* ticket's status, not the backport ticket's
- Hand-merging a conflict without checking for a missing prerequisite first
- Cherry-picking with rerere on, then trusting a resolution you never looked at
- `gh stack init` against a stale local `release/x.y.z` ref — the stack records the wrong base
- Reporting a backport done without `go build`/`go vet`, or implying integration tests ran when they didn't
- Leaving the auto-generated PR body or the repo PR template in place
- Silently dropping part of the upstream commit because the file doesn't exist on the release branch — find where that code lives on the branch, or say you dropped it

## Common mistakes

| Mistake | Fix |
|---|---|
| Backport ticket key used for the upstream ticket in the title | Title always carries the backport ticket; body's cherry-pick line carries the upstream PR |
| Upstream title pasted whole, keeping its `CBG-nnnn:` prefix | Strip it — one ticket reference per title |
| `cherry-pick --continue` leaves the upstream commit subject | `git commit --amend` to the backport message |
| Dropping an upstream test file that has no counterpart on the branch | Apply its changes to wherever that test lives on the release branch |
| Marking a PR clean after fixing compile errors | Any post-cherry-pick edit makes it unclean; list it |
