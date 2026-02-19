pipeline {
    agent { label 'sgw-pipeline-ec2' }

    options {
        timeout(time: 60, unit: 'MINUTES')
        disableConcurrentBuilds(abortPrevious: true)
    }

    environment {
        BRANCH = "${BRANCH_NAME}"
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
        EE_BUILD_TAG = 'cb_sg_enterprise'
        SGW_REPO = 'github.com/couchbase/sync_gateway'
        GH_ACCESS_TOKEN_CREDENTIAL = 'github_cb-robot-sg_access_token'
    }

    stages {
        stage('SCM') {
            steps {
                sh 'git rev-parse HEAD > .git/commit-id'
                script {
                    env.SG_COMMIT = readFile '.git/commit-id'
                    // Set BRANCH variable to target branch if this build is a PR
                    if (env.CHANGE_TARGET) {
                        env.BRANCH = env.CHANGE_TARGET
                    }
                }
                // forces go to get private modules via ssh
                sh 'git config --global url."git@github.com:".insteadOf "https://github.com/"'
            }
        }
        stage('Setup') {
            stages {
                stage('Go Modules') {
                    steps {
                        script {
                            // bootstrap a go version from go.mod. This requires a new enough version of go to run golang.org/dl/go$ver
                            env.GO_VERSION = 'go' + sh(
                              returnStdout: true,
                              script: '''
                                go list -m -f '{{.GoVersion}}'
                              '''
                            ).trim()
                            sh '''
                              set -eux

                              echo "Sync Gateway go.mod version is $GO_VERSION"
                              go install "golang.org/dl/$GO_VERSION@latest"
                              ~/go/bin/$GO_VERSION download
                            '''
                            env.GOROOT = sh(
                              returnStdout: true,
                              script: '~/go/bin/$GO_VERSION env GOROOT'
                            ).trim()
                            env.GOTOOLS = sh(
                              returnStdout: true,
                              script: '~/go/bin/$GO_VERSION env GOPATH'
                            ).trim()
                            env.PATH = "${env.GOROOT}/bin:${env.PATH}"
                            sh '''
                              which go
                              go version
                              go env
                            '''
                            sshagent(credentials: ['CB_SG_Robot_Github_SSH_Key']) {
                                sh '''
                                [ -d ~/.ssh ] || mkdir ~/.ssh && chmod 0700 ~/.ssh
                                ssh-keyscan -t rsa,dsa github.com >> ~/.ssh/known_hosts
                            '''
                                sh "go get -v -tags ${EE_BUILD_TAG} ./..."
                            }
                        }
                    }
                }
                stage('Go Tools') {
                    steps {
                        // goveralls is used to send coverprofiles to coveralls.io
                        sh 'go install github.com/mattn/goveralls@latest'
                        sh 'go install gotest.tools/gotestsum@latest'
                    }
                }
            }
        }

        stage('Builds') {
            parallel {
                stage('CE Linux') {
                    steps {
                        sh "GOOS=linux go build -o sync_gateway_ce-linux -v ${SGW_REPO}"
                    }
                }
                stage('EE Linux') {
                    steps {
                        sh "GOOS=linux go build -o sync_gateway_ee-linux -tags ${EE_BUILD_TAG} -v ${SGW_REPO}"
                    }
                }
                stage('Windows Service') {
                    steps {
                        sh "GOOS=windows go build -o sync_gateway_ce-windows-service -v ${SGW_REPO}/service/sg-windows/sg-service"
                    }
                }
            }
        }

        stage('Tests') {
            parallel {
                stage('Unit') {
                    stages {
                        stage('CE') {
                            when { branch 'main' }
                            steps {
                                script {
                                    // Travis-related variables are required as coveralls.io only officially supports a certain set of CI tools.
                                    withEnv(["PATH+GO=${env.GOTOOLS}/bin", "TRAVIS_BRANCH=${env.BRANCH}", "TRAVIS_PULL_REQUEST=${env.CHANGE_ID}", "TRAVIS_JOB_ID=${env.BUILD_NUMBER}"]) {
                                        githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ce-unit-tests', description: 'CE Unit Tests Running', status: 'PENDING')

                                        sh 'mkdir -p reports'
                                        // --junitfile-project-name is used so that Jenkins doesn't collapse CE and EE results together.
                                        def testExitCode = sh(
                                            script: 'gotestsum --junitfile=test-ce.xml --junitfile-project-name CE --junitfile-testcase-classname relative --format standard-verbose -- -shuffle=on -timeout=20m -coverpkg=./... -coverprofile=cover_ce.out -race -count=1 ./... > verbose_ce.out',
                                            returnStatus: true,
                                        )

                                        // convert the junit file to prepend CE- to all test names to differentiate from EE tests
                                        sh '''xmlstarlet ed -u '//testcase/@classname' -x 'concat("CE-", .)' test-ce.xml > reports/test-ce.xml'''

                                        // Print total coverage stats
                                        sh 'go tool cover -func=cover_ce.out | awk \'END{print "Total SG CE Coverage: " $3}\''

                                        // Grab test fail/total counts so we can print them later
                                        sh "grep '\\-\\-\\- PASS: ' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-pass.count"
                                        sh "grep '\\-\\-\\- FAIL: ' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-fail.count"
                                        sh "grep '\\-\\-\\- SKIP: ' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-skip.count"
                                        sh "grep '=== RUN' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-total.count"
                                        env.TEST_CE_PASS = readFile 'test-ce-pass.count'
                                        env.TEST_CE_FAIL = readFile 'test-ce-fail.count'
                                        env.TEST_CE_SKIP = readFile 'test-ce-skip.count'
                                        env.TEST_CE_TOTAL = readFile 'test-ce-total.count'

                                        if (testExitCode == 0) {
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ce-unit-tests', description: env.TEST_CE_PASS + '/' + env.TEST_CE_TOTAL + ' passed (' + env.TEST_CE_SKIP + ' skipped)', status: 'SUCCESS')
                                    } else {
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ce-unit-tests', description: env.TEST_CE_FAIL + '/' + env.TEST_CE_TOTAL + ' failed (' + env.TEST_CE_SKIP + ' skipped)', status: 'FAILURE')
                                            // archive verbose test logs in the event of a test failure
                                            archiveArtifacts artifacts: 'verbose_ce.out', fingerprint: false
                                            unstable('At least one CE unit test failed')
                                        }

                                        // Publish CE coverage to coveralls.io
                                        // Replace covermode values with set just for coveralls to reduce the variability in reports.
                                        sh 'awk \'NR==1{print "mode: set";next} $NF>0{$NF=1} {print}\' cover_ce.out > cover_ce_coveralls.out'
                                        sh 'which goveralls' // check if goveralls is installed
                                        sh 'goveralls -coverprofile=cover_ce_coveralls.out -service=uberjenkins -repotoken=$COVERALLS_TOKEN || true'
                                    }
                                }
                            }
                        }

                        stage('EE') {
                            steps {
                                script {
                                    withEnv(["PATH+GO=${env.GOTOOLS}/bin"]) {
                                        githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ee-unit-tests', description: 'EE Unit Tests Running', status: 'PENDING')

                                        sh 'mkdir -p reports'
                                        // --junitfile-project-name is used so that Jenkins doesn't collapse CE and EE results together.
                                        def testExitCode = sh(
                                          script: "gotestsum --junitfile=test-ee.xml --junitfile-project-name EE --junitfile-testcase-classname relative --format standard-verbose -- -shuffle=on -timeout=20m -tags ${EE_BUILD_TAG} -coverpkg=./... -coverprofile=cover_ee.out -race -count=1 ./... 2>&1 > verbose_ee.out",
                                          returnStatus: true,
                                        )

                                        // convert the junit file to prepend EE- to all test names to differentiate from EE tests
                                        sh '''xmlstarlet ed -u '//testcase/@classname' -x 'concat("EE-", .)' test-ee.xml > reports/test-ee.xml'''

                                        sh 'go tool cover -func=cover_ee.out | awk \'END{print "Total SG EE Coverage: " $3}\''

                                        // Grab test fail/total counts so we can print them later
                                        sh "grep '\\-\\-\\- PASS: ' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-pass.count"
                                        sh "grep '\\-\\-\\- FAIL: ' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-fail.count"
                                        sh "grep '\\-\\-\\- SKIP: ' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-skip.count"
                                        sh "grep '=== RUN' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-total.count"
                                        env.TEST_EE_PASS = readFile 'test-ee-pass.count'
                                        env.TEST_EE_FAIL = readFile 'test-ee-fail.count'
                                        env.TEST_EE_SKIP = readFile 'test-ee-skip.count'
                                        env.TEST_EE_TOTAL = readFile 'test-ee-total.count'

                                        // Generate junit-formatted test report
                                        if (testExitCode == 0) {
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ee-unit-tests', description: env.TEST_EE_PASS + '/' + env.TEST_EE_TOTAL + ' passed (' + env.TEST_EE_SKIP + ' skipped)', status: 'SUCCESS')
                                    } else {
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ee-unit-tests', description: env.TEST_EE_FAIL + '/' + env.TEST_EE_TOTAL + ' failed (' + env.TEST_EE_SKIP + ' skipped)', status: 'FAILURE')
                                            // archive verbose test logs in the event of a test failure
                                            archiveArtifacts artifacts: 'verbose_ee.out', fingerprint: false
                                            unstable('At least one EE unit test failed')
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                stage('Integration') {
                    stages {
                        stage('main') {
                            when { branch 'main' }
                            steps {
                                echo 'Queueing Integration test for branch "main" ...'
                                // Queues up an async integration test run using default build params (main branch),
                                // but waits up to an hour for batches of PR merges before actually running (via quietPeriod)
                                build job: 'MainIntegration', quietPeriod: 3600, wait: false
                            }
                        }
                    }
                }
            }
        }
        stage('Benchmarks') {
            when { branch 'main' }
            steps {
                echo 'Queueing Benchmark Run test for branch "main" ...'
            // TODO: Add this back with new system
            // build job: 'sync-gateway-benchmark', parameters: [string(name: 'SG_COMMIT', value: env.SG_COMMIT)], wait: false
            }
        }
    }

    post {
        always {
            // record with general Coverage plugin and push coverage back out to GitHub
            discoverGitReferenceBuild() // required before recordCoverage to infer base branch/commit for PR
            recordCoverage(tools: [[parser: 'GO_COV', pattern: 'cover*out']])
            // Publish the junit test reports
            junit allowEmptyResults: true, testResults: 'reports/test-*.xml'
        }
        unstable {
            // archive non-verbose outputs upon failure for inspection (each verbose output is conditionally archived on stage failure)
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'main') {
                    slackSend color: 'danger', message: "Failed tests in main SGW pipeline: ${currentBuild.fullDisplayName}\nAt least one test failed: ${env.BUILD_URL}"
                }
            }
        }
        failure {
            // archive non-verbose outputs upon failure for inspection (each verbose output is conditionally archived on stage failure)
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'main') {
                    slackSend color: 'danger', message: "Build failure!!!\nA build failure occurred in the main SGW pipeline: ${currentBuild.fullDisplayName}\nSomething went wrong building: ${env.BUILD_URL}"
                }
            }
        }
        aborted {
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'main') {
                    slackSend color: 'danger', message: "main SGW pipeline build aborted: ${currentBuild.fullDisplayName}\nCould be due to build timeout: ${env.BUILD_URL}"
                }
            }
        }
        cleanup {
            cleanWs(disableDeferredWipeout: true)
        }
    }
}
