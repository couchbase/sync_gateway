//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// Shared Slack notification helpers, loaded via the `load` step by Jenkinsfiles that want
// consistent Slack formatting (see Jenkinsfile and integration-test/e2e/Jenkinsfile).

// Builds a Slack-friendly summary of the JUnit results recorded by the 'junit' step in post.always,
// including up to 10 failed test names (Jenkins runs post.always before success/failure/unstable/aborted,
// so results are available here).
def testSummary() {
    def testResultAction = currentBuild.rawBuild.getAction(hudson.tasks.test.AbstractTestResultAction)
    if (testResultAction == null) {
        return '_No test results found_'
    }
    def summary = "*Tests:* ${testResultAction.totalCount} total, ${testResultAction.failCount} failed, ${testResultAction.skipCount} skipped"
    if (testResultAction.failCount > 0) {
        def failedTests = testResultAction.getFailedTests()
        def names = failedTests.take(10).collect { "• ${it.fullDisplayName}" }.join('\n')
        summary += "\n*Failed tests:*\n${names}"
        if (failedTests.size() > 10) {
            summary += "\n• ...and ${failedTests.size() - 10} more"
        }
    }
    return summary
}

// Builds a Slack message: a status emoji/title line, an optional block of key/value details
// (insertion order preserved, so pass a LinkedHashMap / map literal), the JUnit test summary, and
// a trailing link.
def slackMessage(String title, String status, String link, Map details = [:]) {
    def emoji = status == 'passed' ? ':white_check_mark:' : ':x:'
    def lines = ["${emoji} *${title} ${status}* — ${currentBuild.fullDisplayName}"]
    if (details) {
        lines << ''
        details.each { key, value -> lines << "*${key}:* ${value}" }
    }
    lines << ''
    lines << testSummary()
    lines << ''
    lines << link
    return lines.join('\n')
}

// Formats a commit SHA as a Slack link to its GitHub commit page, showing the short SHA as the
// link text. Returns 'n/a' if commit is null/empty (e.g. GIT_COMMIT wasn't captured).
def githubCommitLink(String commit) {
    if (!commit) {
        return 'n/a'
    }
    return "<https://github.com/couchbase/sync_gateway/commit/${commit}|${commit.take(8)}>"
}

// Looks for a CBG-<digits> ticket reference (e.g. from a branch name like 'torcolvin/CBG-1234-fix')
// and formats it as a Slack link to the corresponding Jira issue. Returns null if no match is found.
def jiraLinkForBranch(String branch) {
    if (!branch) {
        return null
    }
    def matcher = (branch =~ /(?i)CBG-(\d+)/)
    if (!matcher.find()) {
        return null
    }
    def ticket = "CBG-${matcher.group(1)}"
    return "<https://jira.issues.couchbase.com/browse/${ticket}|${ticket}>"
}

// Looks up the Slack member ID of whoever manually triggered this build in the UI, via
// .github/slack_usernames.yaml (Jenkins usernames are identical to GitHub usernames in this org).
// Returns null for non-user-triggered builds (e.g. an automatic fan-out from an upstream job,
// which has an UpstreamCause instead) or if the triggering user has no entry/Slack ID in the map.
def slackUserIdForBuild() {
    def userIdCauses = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')
    if (!userIdCauses) {
        echo('No UserIdCause on this build (not manually triggered) - skipping Slack DM')
        return null
    }
    if (!fileExists('.github/slack_usernames.yaml')) {
        echo('.github/slack_usernames.yaml not present (build likely failed before it could be fetched) - skipping Slack DM')
        return null
    }
    def githubUsername = userIdCauses[0].userId
    def slackMap = readYaml(file: '.github/slack_usernames.yaml')
    def slackUserId = slackMap[githubUsername]
    if (!slackUserId) {
        echo("No Slack ID mapped for '${githubUsername}' in .github/slack_usernames.yaml")
        return null
    }
    return slackUserId
}

// Sends a failure-style Slack notification: always to #syncgatewaybot; additionally either DMs
// the user who manually triggered the build (if known) or posts to #syncgatewaybot-alerts (if
// triggered by a pipeline rather than a person).
def slackSendFailure(String title, String status, String link, Map details = [:]) {
    def message = slackMessage(title, status, link, details)
    def dmTarget = slackUserIdForBuild()
    if (dmTarget) {
        slackSend(channel: dmTarget, color: 'danger', message: message)
    } else {
        slackSend(channel: 'syncgatewaybot-alerts', color: 'danger', message: message)
    }
    slackSend(channel: 'syncgatewaybot', color: 'danger', message: message)
}

return this
