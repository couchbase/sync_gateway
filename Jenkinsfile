pipeline {
    agent { label 'sgw-pipeline-ec2' }

    options {
        timeout(time: 60, unit: 'MINUTES')
    }

    environment {
        BRANCH = "${BRANCH_NAME}"
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
        EE_BUILD_TAG = "cb_sg_enterprise"
        SGW_REPO = "github.com/couchbase/sync_gateway"
        GH_ACCESS_TOKEN_CREDENTIAL = "github_cb-robot-sg_access_token"
        GO111MODULE = "on"
        GOCACHE = "${WORKSPACE}/.gocache"
    }

    tools {
        go '1.24.2'
    }

    stages {
        stage('SCM') {
            steps {
                sh "git rev-parse HEAD > .git/commit-id"
                script {
                    env.SG_COMMIT = readFile '.git/commit-id'
                    // Set BRANCH variable to target branch if this build is a PR
                    if (env.CHANGE_TARGET) {
                        env.BRANCH = env.CHANGE_TARGET
                    }
                    env.GOTOOLS = sh(returnStdout: true, script: 'go env GOPATH').trim()
                }
                // forces go to get private modules via ssh
                sh 'git config --global url."git@github.com:".insteadOf "https://github.com/"'
            }
        }
        stage('Setup') {
            stages {
                stage('Go Modules') {
                    steps {
                        sh "which go"
                        sh "go version"
                        sh "go env"
                        sshagent(credentials: ['CB SG Robot Github SSH Key']) {
                            sh '''
                                [ -d ~/.ssh ] || mkdir ~/.ssh && chmod 0700 ~/.ssh
                                ssh-keyscan -t rsa,dsa github.com >> ~/.ssh/known_hosts
                            '''
                            sh "go get -v -tags ${EE_BUILD_TAG} ./..."
                        }
                    }
                }
                stage('Go Tools') {
                    steps {
                        // unhandled error checker
                        sh 'go install github.com/kisielk/errcheck@latest'
                        // goveralls is used to send coverprofiles to coveralls.io
                        sh 'go install github.com/mattn/goveralls@latest'
                        // Jenkins coverage reporting tools
                        sh 'go install github.com/axw/gocov/gocov@latest'
                        sh 'go install github.com/AlekSi/gocov-xml@latest'
                        // Jenkins test reporting tools
                        sh 'go install github.com/tebeka/go2xunit@latest'
                    }
                }
            }
        }

        stage('Tests') {
            parallel {
                stage('Unit') {
                    stages {

                        stage('EE') {
                            steps {
                                withEnv(["PATH+GO=${env.GOTOOLS}/bin"]) {
                                    githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ee-unit-tests', description: 'EE Unit Tests Running', status: 'PENDING')

                                    // Build EE coverprofiles
                                    sh "2>&1 go test -shuffle=on -timeout=20m -tags ${EE_BUILD_TAG} -coverpkg=./... -coverprofile=cover_ee.out -count=1 -v ./auth > verbose_ee.out.raw || true"

                                    sh 'go tool cover -func=cover_ee.out | awk \'END{print "Total SG EE Coverage: " $3}\''

                                    sh 'mkdir -p reports'

                                    // strip non-printable characters from the raw verbose test output
                                    sh 'LC_CTYPE=C tr -dc [:print:][:space:] < verbose_ee.out.raw > verbose_ee.out'

                                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                                    sh 'gocov convert cover_ee.out | gocov-xml > reports/coverage-ee.xml'

                                    // Grab test fail/total counts so we can print them later
                                    sh "grep '\\-\\-\\- PASS: ' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-pass.count"
                                    sh "grep '\\-\\-\\- FAIL: ' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-fail.count"
                                    sh "grep '\\-\\-\\- SKIP: ' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-skip.count"
                                    sh "grep '=== RUN' verbose_ee.out | wc -l | awk '{printf \$1}' > test-ee-total.count"
                                    script {
                                        env.TEST_EE_PASS = readFile 'test-ee-pass.count'
                                        env.TEST_EE_FAIL = readFile 'test-ee-fail.count'
                                        env.TEST_EE_SKIP = readFile 'test-ee-skip.count'
                                        env.TEST_EE_TOTAL = readFile 'test-ee-total.count'
                                    }

                                    // Generate junit-formatted test report
                                    script {
                                        try {
                                            sh 'go2xunit -fail -suite-name-prefix="EE-" -input verbose_ee.out -output reports/test-ee.xml'
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ee-unit-tests', description: env.TEST_EE_PASS+'/'+env.TEST_EE_TOTAL+' passed ('+env.TEST_EE_SKIP+' skipped)', status: 'SUCCESS')
                                        } catch (Exception e) {
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ee-unit-tests', description: env.TEST_EE_FAIL+'/'+env.TEST_EE_TOTAL+' failed ('+env.TEST_EE_SKIP+' skipped)', status: 'FAILURE')
                                            // archive verbose test logs in the event of a test failure
                                            archiveArtifacts artifacts: 'verbose_ee.out', fingerprint: false
                                            unstable("At least one EE unit test failed")
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

            }
        }
        stage('Benchmarks'){
            when { branch 'main' }
            steps{
                echo 'Queueing Benchmark Run test for branch "main" ...'
                // TODO: Add this back with new system
                // build job: 'sync-gateway-benchmark', parameters: [string(name: 'SG_COMMIT', value: env.SG_COMMIT)], wait: false
            }
        }
    }

    post {
        always {
            // Publish the cobertura formatted test coverage reports into Jenkins
            //cobertura autoUpdateHealth: false, onlyStable: false, autoUpdateStability: false, coberturaReportFile: 'reports/coverage-*.xml', conditionalCoverageTargets: '70, 0, 0', failNoReports: false, failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', sourceEncoding: 'ASCII', zoomCoverageChart: false
            // record with general Coverage plugin and push coverage back out to GitHub
            discoverGitReferenceBuild() // required before recordCoverage to infer base branch/commit for PR
            recordCoverage(tools: [[parser: 'COBERTURA', pattern: 'reports/coverage-*.xml']],)
        }
        unstable {
            // archive non-verbose outputs upon failure for inspection (each verbose output is conditionally archived on stage failure)
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'main') {
                    slackSend color: "danger", message: "Failed tests in main SGW pipeline: ${currentBuild.fullDisplayName}\nAt least one test failed: ${env.BUILD_URL}"
                }
            }
        }
        failure {
            // archive non-verbose outputs upon failure for inspection (each verbose output is conditionally archived on stage failure)
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'main') {
                    slackSend color: "danger", message: "Build failure!!!\nA build failure occurred in the main SGW pipeline: ${currentBuild.fullDisplayName}\nSomething went wrong building: ${env.BUILD_URL}"
                }
            }
        }
        aborted {
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'main') {
                    slackSend color: "danger", message: "main SGW pipeline build aborted: ${currentBuild.fullDisplayName}\nCould be due to build timeout: ${env.BUILD_URL}"
                }
            }
        }
        cleanup {
            cleanWs(disableDeferredWipeout: true)
	}
    }
}
