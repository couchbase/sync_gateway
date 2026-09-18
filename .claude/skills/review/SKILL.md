---
name: review
description: Use when the user asks for a code review, says /review, or names a PR number, a branch, or working-tree changes to review. Use when the user wants correctness, performance, or concurrency bugs found in a diff.
---

# review

Deep review of a diff. Every bug claim must be proved by a test that fails before the fix and passes after it.

## The Iron Law

**No finding without a reproducing test.**

A finding you cannot reproduce is a guess. Delete it or downgrade it to an open question.

## Never use the built-in review

Do not invoke `/code-review`, `/security-review`, or `/simplify`. Do not delegate to the `code-review` skill or the `superpowers:code-reviewer` agent. This skill replaces them. If you think "the built-in review is faster here", you are rationalising. Follow the steps below.

## Step 1: Resolve the target

If the user gave an argument, that argument wins:

| Argument | Target |
|---|---|
| `#1234` or `1234` | That pull request |
| A branch name | That branch against its merge base |
| A path | Working-tree changes under that path |

If the user gave no argument, pick the first target that has content, in this order:

1. Unstaged and staged working-tree changes (`git status --porcelain`)
2. The current branch against its merge base with the default branch
3. The open pull request for the current branch

State the target you picked in one line before you start.

```bash
# working tree
git diff HEAD
# branch
BASE=$(git merge-base HEAD origin/$(git symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||'))
git diff "$BASE"...HEAD
# pull request
gh pr diff 1234
gh pr view 1234 --json title,body,files,commits
```

## Step 2: Build context before you judge

Do not review a diff in isolation. For each changed function:

1. Read the whole file, not only the hunk.
2. Find every caller and every implementation of the changed interface.
3. Read the tests that cover the changed lines.
4. Read the git history of the changed lines if the change looks like a revert or a re-fix.

A hunk that looks wrong is often correct in context. A hunk that looks correct is often wrong at a call site.

## Step 3: Review passes

Run each pass over the whole diff. Do not merge the passes.

### Pass A: correctness
- Error paths, early returns, and partial failure.
- Nil, empty, zero-value, and boundary inputs.
- Off-by-one, inclusive and exclusive ranges, and truncation.
- Resource lifetime: leaks, double close, use after close, missing defer.
- Context and cancellation: ignored cancellation, wrong parent context.

### Pass B: concurrency
- Shared mutable state reached from more than one goroutine or thread.
- Lock scope: a lock released before the read of the value it protects.
- Lock ordering between two locks. Look for a reversed pair.
- Check-then-act races. A read, a decision, then a write is a race unless one lock covers all three.
- Atomics used for two fields that must change together.
- Channel and callback lifetime: send on a closed channel, blocked send with no reader, callback that fires after teardown.

### Pass C: performance
- Work inside a lock that does not need the lock.
- Allocation, marshalling, or a map copy on a hot path.
- N+1 calls to storage or to a remote service.
- Unbounded growth: a map, a slice, or a queue with no eviction.
- A change in complexity, for example a linear scan added inside a loop.

### Pass D: multi-node behaviour
Run this pass when the project runs as a cluster or shares state between processes. For Couchbase Sync Gateway, this pass is always relevant.

- Two nodes act on the same document in the same bucket. Which write wins, and is the loser detected?
- The change reads or writes state that arrives over a DCP feed. Order between the mutation and the feed event is not guaranteed. Does the code assume it is?
- The change caches bucket state in memory. How does a second node invalidate that cache?
- A rolling upgrade runs old code and new code at the same time. Is the on-disk or in-bucket format readable by both?
- The node restarts mid-operation. Is the operation idempotent on replay?
- A CAS retry loop repeats a side effect that must run once.

### Pass E: comments and defensive code
- Flag comments longer than two lines unless they explain non-obvious logic.
- Flag comments that describe old behaviour or name a ticket.
- For each new nil check, bounds check, or recover: name the concrete caller or event that produces that input. If no caller can produce it, the check hides a bug. Say so.

### Pass F: test coverage
Judge coverage, not style. Do not report naming, table-driven layout, or duplication in tests. Report:
- A changed branch with no test that reaches it.
- A new error path with no test.
- A concurrency change with no test that runs under the race detector.

## Step 4: Prove every finding

For each candidate bug, in order:

1. Write the smallest test that reproduces the bug against the changed code.
2. Run it. If it passes, the finding is wrong. Delete the finding.
3. If it fails, record the exact command and the exact output.
4. Confirm that the same test passes against the pre-change code, or explain why the bug is new.

Rules for these tests:
- Put the test in the package it needs to reach, because most languages require this.
- Name the file so it is easy to delete, for example `zz_review_repro_test.go`.
- Delete the file after you record the output. Keep the source in the report.
- For a race, run the race detector, for example `go test -race -run TestX -count=100 ./pkg/...`.
- For a performance claim, run a benchmark and quote both numbers. A performance claim with no measurement is not a finding.

Findings from Pass E and Pass F need no test. Report them in a separate section.

## Step 5: Report

Order findings by severity, worst first. For each proved bug:

```
### [severity] short title
file.go:123

What breaks: one sentence.
Trigger: the exact inputs or interleaving.
Proof: the test source and the failing output.
Fix: the smallest change that makes the test pass.
```

Then add two short sections: "Comments and defensive code" and "Coverage gaps".

End with the count of findings proved, the count of candidates that failed to reproduce, and every command you ran.

## Red flags

Each of these means stop and go back to Step 4:

- "This is clearly a bug, a test is not needed."
- "The race is hard to reproduce, so I will describe it instead."
- "I will note it as a possible issue."
- "The test framework makes this awkward."
- "I ran out of budget, so here is the list."

A described bug with no proof is noise. Report zero findings rather than unproved findings.

## Quick reference

| Step | Output |
|---|---|
| 1 | One line naming the target |
| 2 | Callers, tests, and history read |
| 3 | Candidate list per pass |
| 4 | Test source and output per candidate |
| 5 | Ranked report |
