//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// Shared GitHub commit status helper, loaded via the `load` step by Jenkinsfiles that report their result
// back to GitHub (see integration-test/*/Jenkinsfile).

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
