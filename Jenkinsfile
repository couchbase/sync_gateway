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
        go '1.23.3'
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
                        stage('CE') {
                            when { branch 'main' }
                            steps{
                                // Travis-related variables are required as coveralls.io only officially supports a certain set of CI tools.
                                withEnv(["PATH+GO=${env.GOTOOLS}/bin", "TRAVIS_BRANCH=${env.BRANCH}", "TRAVIS_PULL_REQUEST=${env.CHANGE_ID}", "TRAVIS_JOB_ID=${env.BUILD_NUMBER}"]) {
                                    // Build CE coverprofiles
                                    sh '2>&1 go test -shuffle=on -timeout=20m -coverpkg=./... -coverprofile=cover_ce.out -race -count=1000 -failfast -v ./rest -run TestCBLRevposHandling > verbose_ce.out.raw || true'

                                    // Print total coverage stats
                                    sh 'go tool cover -func=cover_ce.out | awk \'END{print "Total SG CE Coverage: " $3}\''

                                    sh 'mkdir -p reports'

                                    // strip non-printable characters from the raw verbose test output
                                    sh 'LC_CTYPE=C tr -dc [:print:][:space:] < verbose_ce.out.raw > verbose_ce.out'

                                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                                    sh 'which gocov' // check if gocov is installed
                                    sh 'gocov convert cover_ce.out | gocov-xml > reports/coverage-ce.xml'

                                    // Grab test fail/total counts so we can print them later
                                    sh "grep '\\-\\-\\- PASS: ' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-pass.count"
                                    sh "grep '\\-\\-\\- FAIL: ' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-fail.count"
                                    sh "grep '\\-\\-\\- SKIP: ' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-skip.count"
                                    sh "grep '=== RUN' verbose_ce.out | wc -l | awk '{printf \$1}' > test-ce-total.count"
                                    script {
                                        env.TEST_CE_PASS = readFile 'test-ce-pass.count'
                                        env.TEST_CE_FAIL = readFile 'test-ce-fail.count'
                                        env.TEST_CE_SKIP = readFile 'test-ce-skip.count'
                                        env.TEST_CE_TOTAL = readFile 'test-ce-total.count'
                                    }

                                    // Generate junit-formatted test report
                                    script {
                                        try {
                                            sh 'which go2xunit' // check if go2xunit is installed
                                            sh 'go2xunit -fail -suite-name-prefix="CE-" -input verbose_ce.out -output reports/test-ce.xml'
                                        } catch (Exception e) {
                                            // archive verbose test logs in the event of a test failure
                                            archiveArtifacts artifacts: 'verbose_ce.out', fingerprint: false
                                            unstable("At least one CE unit test failed")
                                        }
                                    }

                                    // Publish CE coverage to coveralls.io
                                    // Replace covermode values with set just for coveralls to reduce the variability in reports.
                                    sh 'awk \'NR==1{print "mode: set";next} $NF>0{$NF=1} {print}\' cover_ce.out > cover_ce_coveralls.out'
                                    sh 'which goveralls' // check if goveralls is installed
                                    sh 'goveralls -coverprofile=cover_ce_coveralls.out -service=uberjenkins -repotoken=$COVERALLS_TOKEN || true'
                                }
                            }
                        }

                        stage('EE') {
                            steps {
                                withEnv(["PATH+GO=${env.GOTOOLS}/bin"]) {

                                    // Build EE coverprofiles
                                    sh "2>&1 go test -shuffle=on -timeout=20m -tags ${EE_BUILD_TAG} -coverpkg=./... -coverprofile=cover_ee.out -race -count=1000 -failfast -v ./rest -run TestCBLRevposHandling > verbose_ee.out.raw || true"

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
                                        } catch (Exception e) {
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
            cobertura autoUpdateHealth: false, onlyStable: false, autoUpdateStability: false, coberturaReportFile: 'reports/coverage-*.xml', conditionalCoverageTargets: '70, 0, 0', failNoReports: false, failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', sourceEncoding: 'ASCII', zoomCoverageChart: false
            // record with general Coverage plugin and push coverage back out to GitHub
            discoverGitReferenceBuild() // required before recordCoverage to infer base branch/commit for PR
            recordCoverage(tools: [[parser: 'COBERTURA', pattern: 'reports/coverage-*.xml']])

            // Publish the junit test reports
            junit allowEmptyResults: true, testResults: 'reports/test-*.xml'
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
            sh "go clean -cache"
	}
    }
}
