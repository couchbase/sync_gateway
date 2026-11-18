---
name: sync-gateway-flaky-test-ticket
description: 'Use when the user wants a CBG Jira ticket filed for a flaky or intermittently failing Sync Gateway test. The user gives a Jenkins build link or a GitHub Actions run link. The ticket is titled "Flaky test: <TestName>" and its description has the link and the failure output of the test. Use for requests like "file a flaky test ticket for this", "open a ticket for TestFoo, it failed here: <url>", "make a CBG for this flake", or a pasted CI link with "flaky". Not for fixing the test, and not for backport tickets (that is sync-gateway-backport-tickets).'
---

# Sync Gateway Flaky Test Ticket

File one CBG ticket for one flaky test. The ticket records where the test failed and what it printed, so that someone can investigate later. Do not investigate the cause and do not change code. CBG-6064 is a reference example of the description layout.

## Get the inputs

You need a link to the failed run. Accept these forms:

- Jenkins: `https://jenkins.sgwdev.com/job/<folder>/job/<name>/<build>/...`. The job full name is the `job/` segments joined with `/` (for example `sgw-unix-build/4.2.0`). The build number is the first number after the job segments. A `testReport/...` suffix names the test.
- GitHub Actions: `https://github.com/<owner>/<repo>/actions/runs/<run-id>` with an optional `/job/<job-id>`.

The user can name the test. If they do not, take the test name from the link (Jenkins `testReport` path) or from the failed tests of the run. If the run has more than one failed test, ask which tests to file, with AskUserQuestion. File one ticket per test.

## Read the failure

For Jenkins:

1. Call `getBuild` for the job and build. Take the branch (usually the last job segment), the date (`timestamp`), and the result.
2. Call `getTestResults` with `onlyFailingTests: true`. Find the record of the test. Take its error details, stack trace, and stdout.
3. If the test results do not have the output, use the archived `verbose*.out` file of the build (for example `verbose_ee.out` or `verbose_int.out`). Do not use the console log, because the Jenkins jobs filter the test output out of it. The `artifacts` of `getBuild` list the file, and its URL is `<build url>artifact/<relativePath>`. The Jenkins tools cannot download an artifact. Ask the user to save the file to the scratchpad or to paste the block of the test. Then take the block as in step 3 for GitHub Actions.

For GitHub Actions:

1. Run `gh run view <run-id> --repo <owner>/<repo> --json displayTitle,headBranch,createdAt,jobs`.
2. Run `gh run view <run-id> --repo <owner>/<repo> --log-failed` and write the output to a file in the scratchpad. The log can be large, so do not print it.
3. Take the block for the test from the file: the lines from `=== RUN   <TestName>` to `--- FAIL: <TestName>`, with the subtest failures. Remove the GitHub job and step prefix and the timestamp at the start of each line.

Also take, where the job name or log shows it: Enterprise or Community Edition, the operating system, and Rosmar or Couchbase Server.

### What output goes in the ticket

The ticket needs the failure output of the test, not the full build log:

- Keep the `--- FAIL:` lines for the test and its subtests, and the assertion blocks (`Error Trace`, `Error`, `Test`, `Messages`).
- Keep a panic and its goroutine stack trace for the test goroutine. For a test timeout, keep the `panic: test timed out` line and the stack of the test that hung.
- Keep the log lines of the test just before the failure. These lines help someone to see the problem. About 50 lines is a good default.
- If you remove lines, put `... (N lines removed) ...` where they were. Do not change the lines that you keep.

Jira rejects a description over about 32,000 characters. Keep the description well under that.

## Check for an existing ticket

Search open and closed tickets before you create:

```
project = CBG AND summary ~ "<TestName>" ORDER BY created DESC
```

Request only `["summary", "status", "resolution", "fixVersions", "description"]`. Ignore backport tickets (summary starts with `[x.y.z backport]`). For each result, compare its description with the new failure. A matching failure is the same test, at the same assertion, with the same error text. For example, the `Error:` line and the file and line are the same. Numbers that change from run to run, such as CAS values, counts, or timestamps, do not make a different failure.

- **Open ticket for the test:** tell the user, and ask whether they want a comment on that ticket instead of a new ticket.
- **Closed ticket with a matching failure:** show the user the key, status, resolution, and fix versions. Also show the matching error lines of both failures. Ask with AskUserQuestion: reopen the earlier ticket, or file a new ticket. A failure after a fix can mean that the fix was not complete, or that a new problem has the same symptom. Only the user can decide which it is.
- **Closed ticket with a different failure:** file a new ticket. Name the earlier ticket in the confirmation, so that the user can link the two tickets.

To reopen a ticket, move it to Reopened (or the nearest open status) with `transitionJiraIssue`. To list the transitions of the ticket, find the operation with the Atlassian `discover` tool. Then add a comment with the new failure, in the same layout as the description in [Write the ticket](#write-the-ticket). Show the user the comment and ask for permission before you reopen or write anything, as in [Confirm before you create](#confirm-before-you-create). Do not change the fields of the earlier ticket.

## Write the ticket

- Summary: `Flaky test: <TestName>`. Use the top-level test name, for example `TestRestampVersionCASMou`, not `TestRestampVersionCASMou/live_document`. Name the subtest in the description.
- Description, in Markdown:

  ````
  `<TestName/subtest>` failed in <link>, branch <branch>, <edition>, on <OS> with <Rosmar or Couchbase Server>, on <YYYY-MM-DD>.

  ```
  <failure output>
  ```
  ````

  Put the link as a bare URL, because Jira turns a bare URL into a link. Leave out a detail that the build does not show. Do not guess it. Do not add a cause or an analysis.

- Fields:
  - Project `CBG`, issue type `Bug`, priority `Major`.
  - Component `SyncGateway`: `components: [{"id": "12242"}]`.
  - Labels `flaky-test` and `unit-test`.
  - Security level Private: `security: {"id": "11004"}`. CI logs can have internal host names and paths, so the ticket must not be public.
  - No assignee: Jira sets the project default. No fixVersion and no sprint.

Use cloud ID `jira.issues.couchbase.com` for the Atlassian tools.

Before you create, call `getJiraIssueTypeMetaWithFields` with `projectIdOrKey: "CBG"`, the `Bug` issue type ID, and `requiredFieldsOnly: false`. The result is large and goes to a file, so read the field IDs with `jq -r '.fields[] | .fieldId' <file>`. If `security` is not in the list, stop and tell the user. Without `security` on the create screen, the ticket is public until a later edit sets the security level.

## Confirm before you create

Creating a Jira ticket is outward-facing, and the user wants to read the ticket before it exists. Show the full contents of the ticket in your reply, in this layout:

````
Summary: Flaky test: <TestName>
Fields:  Bug, Major, component SyncGateway, labels flaky-test unit-test, security Private, no assignee

Description:
<the full description, exactly as you will send it>
````

Then ask for permission with AskUserQuestion, as a separate question. Create nothing until the user approves. If the user asks for changes, show the changed ticket and ask again.

## Create and report

Call `createJiraIssue` with `contentFormat: "markdown"`, and put `components` and `security` in `additional_fields`. If the create rejects `security`, stop and tell the user. Never create the ticket without the security level.

Fetch the new ticket with `fields: ["security", "labels", "components"]` and report the values that Jira shows. Give the ticket key and its link `https://jira.issues.couchbase.com/browse/<key>`.
