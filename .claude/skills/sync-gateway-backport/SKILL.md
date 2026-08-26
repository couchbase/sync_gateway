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
   ```
   Everything below works off `origin/release/x.y.z`. A *local* `release/x.y.z` branch is only needed for stacking — see [Stacking](#stacking-dependent-backports).
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
   go build ./... && go vet ./...
   go test -count=1 -run '<affected tests>' ./<pkg>/          # every package the commit touched
   golangci-lint run --config=.golangci-strict.yml --new-from-rev=origin/release/x.y.z ./<pkg>/...
   golangci-lint fmt --config=.golangci-strict.yml --diff ./<pkg>/...
   ```
   Always pass `--config=.golangci-strict.yml` — that is the config CI runs. Without it, golangci-lint falls back to `.golangci.yml`, a different rule set, so a clean local run proves nothing.

   `--new-from-rev` is what makes lint usable — it reports only what this backport adds, instead of the base branch's hundreds of pre-existing findings. Lint every touched package, not just one.

   `golangci-lint run` does **not** check formatting, so `fmt --diff` is a separate step. It applies the repo's goimports rules and exits non-zero on a diff. Do not substitute `gofmt -l`: it ignores import grouping, and it exits 0 even when it lists unformatted files, so it gates nothing.

   A new finding that exists identically on `main`, and is only "new" because the file is new to the release branch: fix it and list it as a deviation. A red CI is worse than an extra bullet in the body.

   Integration tests need `SG_TEST_BACKING_STORE=Couchbase`. If you can't run them, say so explicitly rather than implying they passed.

## Prerequisites

A conflict — or a hunk that calls something the release branch doesn't have — usually means the release branch is missing a change the backport builds on. Find it before resolving by hand, and before concluding that anything has to be dropped:

```bash
git log --oneline origin/main -L :<conflicted-function>:<file>   # what touched this before
git log --oneline -1 -S'<missing symbol>' origin/main -- <dir>/  # which commit introduced a helper or util
git log --oneline origin/release/x.y.z -i --grep='CBG-<candidate>'  # is it on the branch?
```

| Prerequisite state | Action |
|---|---|
| Has its own backport ticket | Backport it as its own layer, stacked below |
| No backport ticket, low risk — test-only, or a small self-contained code change | Backport it anyway as its own layer; title it with the **upstream** ticket key; tell the user it has no backport ticket and offer to create the clone |
| No backport ticket, higher risk — behaviour change, wide blast radius, or you can't tell | **Stop and ask** (AskUserQuestion): backport the prerequisite / adapt the change to the release branch without it / skip the backport |

Adapting around a missing prerequisite is legitimate but costs more and diverges from upstream: every adaptation is a place the branches can drift. If you adapt, list each deviation in the PR body. **Re-run the cherry-pick once the prerequisite is in place** — conflicts frequently vanish entirely and the diff becomes upstream-identical, which is worth far more than a clever manual merge.

### The testify import rename is not a blocker

`main` uses the repo's own assertion wrappers, `github.com/couchbase/sync_gateway/testing/assert` and `github.com/couchbase/sync_gateway/testing/require`; older release branches still import `github.com/stretchr/testify/{assert,require}` directly. A commit that touches a test file converted upstream will therefore conflict on the import block, and often on nothing else.

**That is not a missing prerequisite and not a reason to stop.** The wrappers are API-compatible with testify at every call site the repo uses, so resolve it by keeping the release branch's testify imports and taking the upstream side of everything else:

```bash
git diff --name-only --diff-filter=U          # what conflicted
git diff HEAD -- <file>                        # confirm the import block is the whole of it
```

Do not backport the wrapper packages themselves to make the conflict go away — that is a branch-wide change riding in on an unrelated ticket. Do not drop the hunk either. Rename the imports in the cherry-picked file to whatever that branch already uses, keep the assertions as they are, and note it as a deviation:

```
- `rest/attachment_test.go` — release branch predates the `testing/require` wrappers; imports renamed back to `github.com/stretchr/testify/require`
```

If the conflict is *only* the import lines, this makes the backport routine — but it is still an edit after the cherry-pick, so the PR is unclean, not clean. Only escalate if upstream also uses a wrapper helper that testify has no equivalent for; that is a real prerequisite and goes through the table above.

### Dropping part of an upstream commit is never your call

A hunk that won't apply because a symbol it uses is missing — a test helper in the same package, a `base` utility, a changed signature — is a missing prerequisite, not a file that doesn't exist on the branch. Run the lookup above on the *missing symbol* first. The two cheapest cases are also the two most common, and the table above already classifies both as low risk to stack: a helper defined in the same test package, and a `*_testing.go` utility in `base`.

If you still believe a hunk should be dropped, **stop and ask** (AskUserQuestion). Recording the drop in the commit message and the PR body is not a substitute for asking — the reviewer meets the decision after it is made, on a branch that looks finished. Offer the real options: backport the prerequisite and apply the commit whole / drop the hunk / skip this backport, and say what each one costs.

**Name the tests, not the files.** "Dropped the `admin_api_test.go` half" reads like a packaging detail; "`TestDCPResyncCollectionsStatus` keeps the 1000-doc timing race this commit was removing" is the actual consequence, and it is the sentence that lets a reviewer overrule you. Half-applying a flake fix is the worst version of this: the backport looks done, CI is green on the run you watched, and the flake is still on the branch.

## Stacking dependent backports

Bottom of the stack = the prerequisite; top = the change that needs it.

`gh stack` is a GitHub CLI extension, not a built-in subcommand — install it once, or the first call fails with `unknown command`. The `gh-stack` skill covers the extension in full; only the backport-specific parts are below.

```bash
gh extension list | grep -q gh-stack || gh extension install github/gh-stack
```

`gh stack` rebases onto the **local** trunk branch, so that ref must match `origin` or the stack records a stale base. Update it from a backport branch, never from the trunk itself: `git branch --force` refuses to move the branch you have checked out, so the mistake fails loudly instead of quietly stacking on the wrong base.

```bash
git switch CBG-<lower>
git branch --force release/x.y.z origin/release/x.y.z
git config rerere.enabled true
git config remote.pushDefault origin
gh stack init --base release/x.y.z CBG-<lower> CBG-<upper>
gh stack view --json          # never bare `gh stack view` - it opens a TUI
gh stack submit --auto --open # pushes, creates non-draft PRs, links the stack
```

**Both flags are mandatory, and `--open` does not mean what it looks like.** It marks the PRs *ready for review* — it does not open a browser. With `--auto` alone, every new PR is silently created as a **draft**, which no reviewer will pick up. There is no warning in the output; the PRs just sit there. If it happens anyway, recover with `gh pr ready <N>` per PR — the drafts are otherwise fine and need no re-push.

`gh stack submit` sets each PR's base to the branch below it. Fix the titles and bodies afterwards (`gh pr edit N --title ... --body-file ...`) — `--auto` derives both from the commit message, so every PR in the stack still needs the real body with its attribution footer, and any `(test-only)` title suffix has to be added back.

To extend a stack that is already submitted, do **not** re-run `gh stack init` — it fails with `branch "…" already exists in a stack`. Check out the current top branch, run `gh stack add CBG-<new>` to create and register the new branch, then cherry-pick onto it and `gh stack submit --auto --open` again. Existing PRs are reported as up to date and only the new one is created.

When a branch name is already taken (usually the upstream PR used the same ticket key), suffix the release: `CBG-5414-4.1.2`.

## PR title and body

Title — the ticket is the **backport** ticket, the subject is the **upstream PR title with its own ticket prefix stripped**, and a test-only backport carries a `(test-only)` suffix:

```
[4.1.2 Backport] CBG-5591: stop indexes being built on default when not required
[4.1.2 Backport] CBG-5603: add coverage for default-collection index skip (test-only)
```

The suffix is the only addition allowed to the title — do not use it to summarise anything else. See [The `test-only` marker](#the-test-only-marker) for when it applies.

Body — exactly this shape, nothing else (no repo PR template). The last line is the attribution footer, and it is **not optional** — a reviewer must be able to tell at a glance that a skill opened this PR:

```
CBG-5591

Clean cherry pick of #8495 to 4.1.2

test-only

🤖 Opened with the `sync-gateway-backport` skill in [Claude Code](https://claude.com/claude-code)
```

For anything that did not apply cleanly:

```
CBG-5591

Unclean cherry pick of #8495 to 4.1.2
Changes from main commit:
- `rest/manualbucketpooltest/database_init_manager_test.go` — package doesn't exist on 4.1.2; the signature change was applied to the copy of that test in `rest/` instead
- `rest/database_init_manager_test.go` — `dbConfig.setup()` takes an extra `forcePerBucketAuth` arg on 4.1.2

🤖 Opened with the `sync-gateway-backport` skill in [Claude Code](https://claude.com/claude-code)
```

One bullet per deviation, naming the file and the reason. "Clean" means the cherry-pick applied with no conflicts *and* you changed nothing afterwards — a compile fix still makes it unclean. For a stacked PR, add a line naming the PR it sits on.

### The `test-only` marker

`test-only` tells a reviewer the PR carries no production risk, so it earns a much lighter review. It goes in two places, and they must agree:

- the title, as a `(test-only)` suffix — this is what makes a low-risk PR obvious in a list view
- the body, on its own line between the cherry-pick line (and any deviation bullets) and the footer

Include it only when **every file the branch touches is recognised test code**. Check the real diff, do not judge from the commit subject. List all changed paths — not just `*.go` — and strip the ones the table below classifies as test code:

```bash
git diff --name-only origin/release/x.y.z...HEAD \
  | grep -vE '_test\.go$|_testing\.go$|(^|/)testing/|(^|/)(utilities_testing|main_test_|util_test_|api_test_helpers|jwt_test_utils|replicator_test_helper|leaky_bucket|leaky_datastore)'
```

Empty output → add the line. Any surviving path → leave it off, even for a one-line production change. Do not narrow the command to `-- '*.go'`: a diff of only docs, scripts, CI config, or a Dockerfile would then print nothing and look test-only when it is not. Clean and unclean backports both get it; the two markers are independent — an unclean cherry-pick of a test-only change is still test-only.

**Test-support code counts as test code**, even though it is not named `*_test.go` and does technically compile into the shipped binary. What matters to a reviewer is blast radius: no request path calls it, so a change is very unlikely to reach a deployment. It is not a guarantee — such a file still compiles in, so an `init()` side effect or a new non-test caller would make it production code again. In this repo that means:

| Pattern | Examples |
|---|---|
| `*_test.go` | the ordinary case |
| `*_testing.go` | `base/util_testing.go`, `db/utilities_hlv_testing.go`, `channels/util_testing.go` |
| **everything under the top-level `testing/` package** | all of `testing/assert/`, `testing/require/`, `testing/sgtest/`, and any subpackage added later |
| `utilities_testing*` | the whole `rest/utilities_testing_*.go` family, `base/utilities_testing_rbac.go` |
| `main_test_*`, `util_test_*` | `base/main_test_bucket_pool.go`, `base/util_test_race.go` |
| `*_test_helper*.go`, `*_test_utils.go` | `rest/api_test_helpers.go`, `auth/jwt_test_utils.go`, `rest/replicatortest/replicator_test_helper.go` |
| the leaky bucket fault-injection layer | `base/leaky_bucket.go`, `base/leaky_datastore.go` |

The repo's top-level `testing/` package is test-only in its entirety — every file, every subpackage, no exceptions and nothing to check case by case. It exists solely to be imported by tests; nothing in `base`, `db`, `rest`, or `channels` links it into a running server. A backport whose whole diff lands under `testing/` is `test-only` on that basis alone. The same goes for any new subpackage that appears there later, which is why the grep matches the directory rather than a list of names.

`base/leaky_bucket.go` and `base/leaky_datastore.go` are the odd ones out by name: they carry no test marker at all and live beside production `base` code, but they are the fault-injection wrappers that only `GetTestBucket`-style test setup ever constructs. No request path reaches them — treat them as test code.

One name-based exception, which the grep above deliberately does not filter: **`rest/oidc_test_provider.go` is production code.** It backs the registered `/_oidc_testing` route, so it is reachable in a running deployment. A PR touching it is not test-only whatever its name suggests.

When in doubt about a file the grep does not classify, ask whether any non-test caller reaches it. If yes, it is production.

**A `go.mod` / `go.sum` change always disqualifies `test-only`**, even when every Go file in the diff is test-support code. A dependency bump changes what ships whatever the Go diff looks like, and it is the part of a backport most worth a careful review — the marker would invite the opposite. The command above already prints these paths; never add them to the filter to get an empty result.

The footer goes on every backport PR, including stacked ones, and survives every later `gh pr edit --body-file` — rewriting a body drops it unless you carry it over. Write bodies from a file so the footer is part of the text you author, not something appended by hand:

```bash
gh pr edit <N> --body-file <file>   # file already ends with the footer line
```

## Red flags

- Building a backport branch when the upstream PR hasn't merged — check the *original* ticket's status, not the backport ticket's
- Hand-merging a conflict without checking for a missing prerequisite first
- Cherry-picking with rerere on, then trusting a resolution you never looked at
- `gh stack init` against a stale local `release/x.y.z` ref — the stack records the wrong base
- `gh stack submit --auto` without `--open` — the whole stack lands as drafts and nobody reviews it
- Reporting a backport done without `go build`/`go vet`, or implying integration tests ran when they didn't
- Calling lint clean after a `golangci-lint` run that used the default config instead of `.golangci-strict.yml`, or a format check that can't fail
- Leaving the auto-generated PR body or the repo PR template in place
- Submitting or editing a PR body without the `sync-gateway-backport` attribution footer
- Claiming `test-only` from the commit subject or the ticket instead of the file list — a reviewer who trusts that line skips the production change hidden under it
- Stopping a backport over a testify-vs-wrapper import conflict, or pulling the wrapper packages onto the branch to avoid renaming two import lines
- Dropping part of the upstream commit on your own judgement — a missing helper is a prerequisite question, and the answer is the user's
- Half-applying a flake fix: the tests still on the old timing trick keep flaking, and a green CI run proves nothing about them

## Common mistakes

| Mistake | Fix |
|---|---|
| Backport ticket key used for the upstream ticket in the title | Title always carries the backport ticket; body's cherry-pick line carries the upstream PR |
| Upstream title pasted whole, keeping its `CBG-nnnn:` prefix | Strip it — one ticket reference per title |
| `cherry-pick --continue` leaves the upstream commit subject | `git commit --amend` to the backport message |
| Dropping an upstream test file that has no counterpart on the branch | Apply its changes to wherever that test lives on the release branch |
| A hunk dropped because a helper it calls doesn't exist on the branch | That helper is a prerequisite — find its commit with `git log -S`, stack it, and apply the hunk whole |
| Dropping a hunk and documenting it in the commit/PR body instead of asking | The body explains a decision already made. Ask first with AskUserQuestion |
| Deviation bullet names the file that was dropped | Name the tests that lost the fix, and what they go back to doing |
| Treating a `testing/require` vs `github.com/stretchr/testify/require` import conflict as a blocker or a prerequisite | Rename the imports to the branch's own and take the upstream side of the rest; list it as a deviation |
| Backporting the `testing/assert` / `testing/require` wrapper packages to resolve one import conflict | Branch-wide change on an unrelated ticket — rename the imports in the cherry-picked file instead |
| Marking a PR clean after fixing compile errors | Any post-cherry-pick edit makes it unclean; list it |
| Footer lost when the body is rewritten to add deviations or a stack line | The footer is part of the body template — re-add it as the last line every time |
| `test-only` withheld because the PR touches a test helper, `base/leaky_*.go`, or anything under `testing/` | Test-support code counts as test code, and the whole `testing/` package is test code by definition — see the pattern table. Only a real production file blocks the marker |
| `test-only` on a PR touching `rest/oidc_test_provider.go` | Despite the name it backs the live `/_oidc_testing` route, so it is production |
| `test-only` on a test-support-only diff that also bumps `go.mod` | A dependency bump ships. Check `go.mod`/`go.sum` separately — the `*.go` grep misses it |
| Title says `(test-only)` but the body line is missing, or the reverse | Both come from the same check — set both or neither |
| `(test-only)` suffix dropped by an auto-generated stacked PR title | Re-apply it with `gh pr edit N --title` after `gh stack submit` |
| `--open` dropped from `gh stack submit` because it reads like "open a browser" | It means *ready for review*. Without it `--auto` creates drafts; fix with `gh pr ready <N>` |
| `gh stack init` re-run to add a layer to a live stack | Use `gh stack add <branch>` from the current top branch instead |
