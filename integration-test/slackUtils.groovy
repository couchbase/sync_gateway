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

// Returns a Slack-formatted link to the currently checked-out commit on GitHub, followed by its
// subject line, e.g. "<https://github.com/couchbase/sync_gateway/commit/abc123...|abc1234> Fix the thing".
def gitCommitLink() {
    def sha = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
    def message = sh(script: 'git log -1 --pretty=%s', returnStdout: true).trim()
    def safeMessage = message.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    return "<https://github.com/couchbase/sync_gateway/commit/${sha}|${sha.take(7)}> ${safeMessage}"
}

// Returns a Slack-formatted link to an arbitrary Sync Gateway ref (branch, tag, or commit SHA)
// on GitHub, e.g. "<https://github.com/couchbase/sync_gateway/commit/main|main>".
def refLink(String ref) {
    return "<https://github.com/couchbase/sync_gateway/commit/${ref}|${ref}>"
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

// Returns true if this build is for a pull request, which Jenkins signals by setting CHANGE_ID.
def isPRBuild() {
    return env.CHANGE_ID as boolean
}

// Sends a failure-style Slack notification to #syncgatewaybot, always, as the record of every
// failure, plus at most one of:
//  1. a DM to whoever manually triggered the build, so the person waiting on it hears first - only if
//     slackUserIdForBuild() can identify them, or
//  2. #syncgatewaybot-alerts, to escalate failures from automated builds that aren't against a PR -
//     main-branch and downstream job runs nobody is watching. A PR build never lands here, since its
//     failures are already reported on the PR itself.
def slackSendFailure(String title, String status, String link, Map details = [:]) {
    def message = slackMessage(title, status, link, details)
    def dmTarget = slackUserIdForBuild()
    if (dmTarget) {
        slackSend(channel: dmTarget, color: 'danger', message: message)
    } else if (!isPRBuild()) {
        slackSend(channel: 'syncgatewaybot-alerts', color: 'danger', message: message)
    }
    slackSend(channel: 'syncgatewaybot', color: 'danger', message: message)
}

return this
