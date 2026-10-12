---
name: sync-gateway-backport-tickets
description: Use when the user wants Jira backport tickets created for a CBG ticket. Each backport ticket is a clone of the original, with "[x.y.z backport]" in the summary and the version as fixVersion. Use for requests like "make backport tickets for CBG-1234", "clone CBG-1234 for 4.0.9 and 3.3.9", or "we need to backport this to 4.1.x". Not for doing the code backport itself (that is sync-gateway-backport).
---

# Sync Gateway Backport Tickets

Create one Jira backport ticket per release version for an existing CBG ticket. Each backport ticket is a clone of the original. CBG-5914 is a reference example: its clones are CBG-5915 (4.1.3), CBG-5916 (4.0.9), and CBG-5917 (3.3.9).

## Get the inputs

You need two inputs:

1. The original ticket key, for example `CBG-5914`. Accept a URL and take the key from it.
2. One or more exact release versions, for example `4.1.3` and `4.0.9`.

If the request does not give both, ask with AskUserQuestion. A version must be a full `x.y.z` version, because it becomes the fixVersion. If the user gives `4.1.x` or "the next 4.0 release", ask for the exact version.

Use cloud ID `jira.issues.couchbase.com` for the Atlassian tools.

## Read the original

Fetch the original with `getJiraIssue`, `fields: ["*all"]`, `expand: "names"`, and `responseContentFormat: "markdown"`. The `names` map gives the display name of each `customfield_*` key.

A backport is a full copy of the original, so copy every field that has a value, except these:

- `summary` and `fixVersions`: the backport sets its own.
- `assignee`: Jira gives a new ticket the project default assignee. Do not copy, set, or clear it.
- Sprint and Rank (`customfield_*` fields with these names): backports get scheduled and ranked separately.
- Story Points: set it to `0`, because each backport is sized separately.
- `issuelinks`: no field update can set links. [Copy the links](#copy-the-other-links) with `createIssueLink` after the create.
- `subtasks`: a backport does not take over the subtasks of the original.
- Fields that Jira sets: status, resolution, reporter, creator, created, updated, lastViewed, statuscategorychangedate, votes, watches, worklog, comment, attachment, Development, `timetracking`, `aggregate*` fields, and similar read-only fields.

Copy `security` without fail. Many CBG tickets are Private, and a backport without the security level makes private customer details public.

The tool returns the description as Markdown, even if you ask for ADF (the Jira rich-text JSON format). Smart links come back as `<custom data-type="smartlink" data-id="...">URL</custom>`. Replace each of these tags with its bare URL, because Jira turns a bare URL into a link. Keep the rest of the description as it is.

The Markdown conversion loses some content. Look for @mentions, panels, inline images, tables with merged cells, and code blocks with a language. If the description has any of these, say so in the confirmation, because the user must fix them by hand on each backport.

Also call `getJiraIssueRemoteIssueLinks` for the web links of the original, for example GitHub PRs. No tool can create web links, so list them in the confirmation and in the report. The user then adds them by hand.

If the summary of the original already starts with `[x.y.z backport]`, the user probably gave a backport ticket instead of the original. Stop and ask. Existing tickets use both `backport` and `Backport`, so ignore case when you compare summaries.

## Skip versions that already have a backport

Look at the `issuelinks` of the original for "Cloners" links. A backport exists when the original "is cloned by" a ticket whose summary starts with `[<version> backport]`, in any case. Also search, because someone can create a clone without a link, or edit the summary of a clone:

```
project = CBG AND (issue in linkedIssues(<original key>) OR (fixVersion = "<version>" AND summary ~ "backport"))
```

Request only `["summary", "fixVersions", "issuelinks"]`. Read each result and decide if it is a backport of the original: it links to the original, or its summary is the same work. Do not search with words from the original summary, because quotes and other special characters break the JQL string.

Report each existing backport by key and leave that version out.

## Read the create metadata

Call `getJiraIssueTypeMetaWithFields` with `projectIdOrKey: "CBG"`, the `issuetype.id` of the original, and `requiredFieldsOnly: false`. The result is large and goes to a file, so read it with `jq`:

```bash
jq -r '.fields[] | select(.fieldId=="fixVersions") | .allowedValues[] | "\(.name)\t\(.released)"' <file>
jq -r '.fields[] | .fieldId' <file>
```

The first command lists the versions that exist. A JQL query does not prove that a version exists, because a query for a missing version returns an empty result and no error. If a requested version is not in the list, do not create the version, because that needs project admin rights. Tell the user in the confirmation and leave that version out, unless the user says to create the ticket without a fixVersion.

The second command lists the fields on the create screen. Split the fields to copy into two groups: fields on the create screen, and the other fields.

If the original has a `security` value and `security` is not on the create screen, stop and tell the user. Do not create a ticket. Without `security` on the create screen, the new ticket is public until a later edit sets the security level. If that edit fails, the ticket stays public.

## Read the link direction

Call `getIssueLinkTypes` and find the "Cloners" type. Its `outward` text is "clones" and its `inward` text is "is cloned by". Use these texts with the mapping in [Copy the other links](#copy-the-other-links) to decide the `inwardIssue` and `outwardIssue` of each link before you create any link.

## Confirm before you create

Creating Jira tickets and links is outward-facing. Show the plan and ask for permission with AskUserQuestion before any create call. Show one block per ticket, with each link that the ticket gets:

```
[4.1.3 backport] <original summary>   fixVersion 4.1.3   security <level>
  copied fields: priority, components, labels, <custom field names>
  links: clones CBG-5914, is caused by CBG-1234, relates to MB-56789
```

Each link also shows on the other ticket, so name every linked ticket. Also show the description content that the Markdown conversion loses, and the versions that you skip and why.

Create nothing until the user approves.

## Create each ticket

For each version, call `createJiraIssue`:

- `projectKey`: `CBG`.
- `issueTypeName`: the type of the original (Bug, Task, Improvement).
- `summary`: `[<version> backport] <original summary>`. Use lowercase "backport", as in the reference tickets.
- `description`: the cleaned Markdown description of the original, with `contentFormat: "markdown"`.
- Do not pass an assignee. Jira sets the project default.
- `additional_fields`: `fixVersions: [{"name": "<version>"}]`, plus the copied fields that are on the create screen. Pass option and user fields by `id` (for example `{"security": {"id": "11004"}}`), because names can be ambiguous.

Then set the copied fields that are not on the create screen with one `editJiraIssue` call on the new ticket. If the edit rejects a field, report it and continue. Never remove `security` to get past an error from the create or the edit.

Then link the new ticket to the original with `createIssueLink`:

- `type`: `Cloners`
- `inwardIssue`: the new backport ticket (the ticket that "clones")
- `outwardIssue`: the original ticket (the ticket that "is cloned by")

After the first link, fetch the original and make sure that it shows "is cloned by <new key>". If the direction is reversed, the mapping below is reversed for every link type. Stop and create no more links. Tell the user which link is wrong, because no tool can delete a link and the user must remove it by hand. Then ask whether to continue with `inwardIssue` and `outwardIssue` swapped for all links, copied links included.

### Copy the other links

Copy each issue link of the original to the new ticket, with the same type and the same direction. A `getJiraIssue` link entry names the other ticket as `outwardIssue` or `inwardIssue`. Map it to `createIssueLink` as follows, unless the first Cloners link showed that the direction is reversed:

- Entry has `outwardIssue: X`: the original is the subject of the outward verb (for example "causes X"). Use `inwardIssue` = new ticket, `outwardIssue` = X.
- Entry has `inwardIssue: X`: X is the subject (for example "X clones the original"). Use `inwardIssue` = X, `outwardIssue` = new ticket.

Do not copy the "Cloners" links between the original and its other backport tickets. A backport ticket does not clone its sibling backports.

If a link fails, continue with the other links and list the failure in the report.

## Report

Fetch each new ticket with `fields: ["assignee", "security"]`, and report the assignee and security level that Jira shows. Do not report the values that you expected.

List each ticket that you created as a link, with its version, assignee, and the number of copied links. List the versions that you skipped and why, the fields and links that failed, and the web links that the user must add by hand.

The new tickets have no sprint. The sync-gateway-backport skill finds work with `assignee = currentUser() AND sprint IN openSprints()`. Tell the user that this skill cannot find the new tickets. Each ticket must first get an assignee and a sprint. Example:

```
Created:
- CBG-5915 [4.1.3 backport] ... https://jira.issues.couchbase.com/browse/CBG-5915
Skipped:
- 4.0.9: CBG-5916 already exists
```
