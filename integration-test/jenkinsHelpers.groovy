//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// Shared Jenkins pipeline helpers - Slack notifications, GitHub commit statuses and build metadata -
// loaded via the `load` step by the Jenkinsfiles in this repo (see Jenkinsfile and
// integration-test/*/Jenkinsfile).

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
    def emoji = ['passed': ':white_check_mark:', 'aborted': ':no_entry_sign:'].get(status, ':x:')
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

// Who caused this build - the user who started it, or else the upstream job that fanned it out, as
// "<job> #<build>". Returns '' for a trigger with neither (a timer or the SCM), where there is nobody
// to name. Used in currentBuild.description, so a run's origin shows in the job's build history.
def triggeredBy() {
    def userIdCauses = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')
    if (userIdCauses) {
        return userIdCauses[0].userId ?: ''
    }
    def upstreamCauses = currentBuild.getBuildCauses('hudson.model.Cause$UpstreamCause')
    if (upstreamCauses) {
        return "${upstreamCauses[0].upstreamProject} #${upstreamCauses[0].upstreamBuild}"
    }
    return ''
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

// Returns true if this build was both started by a user and then interrupted by a user - i.e. somebody
// kicked off an adhoc run and cancelled it themselves, so there is nobody who needs telling. An abort with
// any other cause (agent lost, timeout, an automated build cancelled by the system) returns false, since
// nobody chose that and it's worth reporting.
def isSelfCancelledAdhocBuild() {
    if (!currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')) {
        return false
    }
    return currentBuild.rawBuild.getActions(jenkins.model.InterruptedBuildAction).any { action ->
        action.causes.any { it instanceof jenkins.model.CauseOfInterruption.UserInterruption }
    }
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

// Posts a commit status against env.GIT_COMMIT, which shows up on that commit - and on any PR whose head it
// is - in GitHub. `status` is the pipeline's own vocabulary: 'running' -> PENDING, 'passed' -> SUCCESS,
// 'unstable' -> FAILURE, and anything else, including 'aborted', -> ERROR. GitHub has no cancelled state,
// and staying quiet on an abort would strand the pending status on that commit forever.
//
// Reporting is best-effort: none of these jobs gate a merge, so GitHub being unreachable leaves the build
// result untouched rather than failing an otherwise good build. An abort arriving mid-notify still aborts.
// Account, repo and sha are passed explicitly because githubNotify can't reliably infer them from the
// explicit checkouts these jobs do.
def notifyCommitStatus(String context, String description, String status) {
    if (!env.GIT_COMMIT) {
        echo('No GIT_COMMIT captured (checkout failed) - skipping GitHub commit status')
        return
    }
    catchError(message: "Failed to publish GitHub commit status '${context}'", buildResult: 'SUCCESS', stageResult: 'SUCCESS', catchInterruptions: false) {
        githubNotify(
            credentialsId: 'github_cb-robot-sg_access_token',
            account: 'couchbase',
            repo: 'sync_gateway',
            sha: env.GIT_COMMIT,
            context: context,
            description: "${description} ${status}",
            status: ['running': 'PENDING', 'passed': 'SUCCESS', 'unstable': 'FAILURE'].get(status, 'ERROR')
        )
    }
}

return this
