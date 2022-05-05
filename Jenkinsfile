pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    // We could potentially change this to use a dockerfile agent instead so it can be portable.
    agent { label 'sgw-pipeline-ec2' }

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
        go '1.17.5'
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
                stage('CE macOS') {
                    // TODO: Remove skip
                    when { expression { return false } }
                    steps {
                        withEnv(["PATH+GO=${GOPATH}/bin"]) {
                            echo 'TODO: figure out why build issues are caused by gosigar'
                            sh "GOOS=darwin go build -o sync_gateway_ce-darwin -v ${SGW_REPO}"
                        }
                    }
                }
                stage('EE macOS') {
                    // TODO: Remove skip
                    when { expression { return false } }
                    steps {
                        withEnv(["PATH+GO=${GOPATH}/bin"]) {
                            echo 'TODO: figure out why build issues are caused by gosigar'
                            sh "GOOS=darwin go build -o sync_gateway_ee-darwin -tags ${EE_BUILD_TAG} -v ${SGW_REPO}"
                        }
                    }
                }
                stage('CE Windows') {
                    steps {
                        sh "GOOS=windows go build -o sync_gateway_ce-windows -v ${SGW_REPO}"
                    }
                }
                stage('EE Windows') {
                    steps {
                        sh "GOOS=windows go build -o sync_gateway_ee-windows -tags ${EE_BUILD_TAG} -v ${SGW_REPO}"
                    }
                }
                stage('Windows Service') {
                    steps {
                        sh "GOOS=windows go build -o sync_gateway_ce-windows-service -v ${SGW_REPO}/service/sg-windows/sg-service"
                    }
                }
            }
        }

        stage('Checks') {
            parallel {
                stage('gofmt') {
                    steps {
                        script {
                            try {
                                githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-gofmt', description: 'Running', status: 'PENDING')
                                sh "gofmt -d -e . | tee gofmt.out"
                                sh "test -z \"\$(cat gofmt.out)\""
                                githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-gofmt', description: 'OK', status: 'SUCCESS')
                            } catch (Exception e) {
                                sh "wc -l < gofmt.out | awk '{printf \$1}' > gofmt.count"
                                script {
                                    env.GOFMT_COUNT = readFile 'gofmt.count'
                                }
                                githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-gofmt', description: "found "+env.GOFMT_COUNT+" problems", status: 'FAILURE')
                                unstable("gofmt failed")
                            }
                        }
                    }
                }
                stage('go vet') {
                    steps {
                        warnError(message: "go vet failed") {
                            sh "go vet -tags ${EE_BUILD_TAG} ./..."
                        }
                    }
                }
                stage('go fix') {
                    steps {
                        warnError(message: "go fix failed") {
                            sh "go tool fix -diff . | tee gofix.out"
                            sh "test -z \"\$(cat gofix.out)\""
                        }
                    }
                }
                stage('errcheck') {
                    steps {
                        script {
                            try {
                                githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-errcheck', description: 'Running', status: 'PENDING')
                                sh "errcheck ./... | tee errcheck.out"
                                sh "test -z \"\$(cat errcheck.out)\""
                                githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-errcheck', description: 'OK', status: 'SUCCESS')
                            } catch (Exception e) {
                                sh "wc -l < errcheck.out | awk '{printf \$1}' > errcheck.count"
                                script {
                                    env.ERRCHECK_COUNT = readFile 'errcheck.count'
                                }
                                githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-errcheck', description: "found "+env.ERRCHECK_COUNT+" unhandled errors", status: 'FAILURE')
                                unstable("errcheck failed")
                            }
                        }
                    }
                }
            }
        }

        stage('Tests') {
            parallel {
                stage('Unit') {
                    stages {
                        stage('CE') {
                            when { branch 'master' }
                            steps{
                                // Travis-related variables are required as coveralls.io only officially supports a certain set of CI tools.
                                withEnv(["PATH+GO=${env.GOTOOLS}/bin", "TRAVIS_BRANCH=${env.BRANCH}", "TRAVIS_PULL_REQUEST=${env.CHANGE_ID}", "TRAVIS_JOB_ID=${env.BUILD_NUMBER}"]) {
                                    githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ce-unit-tests', description: 'CE Unit Tests Running', status: 'PENDING')

                                    // Build CE coverprofiles
                                    sh '2>&1 go test -timeout=20m -coverpkg=./... -coverprofile=cover_ce.out -race -count=1 -v ./... > verbose_ce.out.raw || true'

                                    // Print total coverage stats
                                    sh 'go tool cover -func=cover_ce.out | awk \'END{print "Total SG CE Coverage: " $3}\''

                                    sh 'mkdir -p reports'

                                    // strip non-printable characters from the raw verbose test output
                                    sh 'LC_CTYPE=C tr -dc [:print:][:space:] < verbose_ce.out.raw > verbose_ce.out'

                                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
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
                                            sh 'go2xunit -fail -suite-name-prefix="CE-" -input verbose_ce.out -output reports/test-ce.xml'
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ce-unit-tests', description: env.TEST_CE_PASS+'/'+env.TEST_CE_TOTAL+' passed ('+env.TEST_CE_SKIP+' skipped)', status: 'SUCCESS')
                                        } catch (Exception e) {
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ce-unit-tests', description: env.TEST_CE_FAIL+'/'+env.TEST_CE_TOTAL+' failed ('+env.TEST_CE_SKIP+' skipped)', status: 'FAILURE')
                                            // archive verbose test logs in the event of a test failure
                                            archiveArtifacts artifacts: 'verbose_ce.out', fingerprint: false
                                            unstable("At least one CE unit test failed")
                                        }
                                    }

                                    // Publish CE coverage to coveralls.io
                                    // Replace covermode values with set just for coveralls to reduce the variability in reports.
                                    sh 'awk \'NR==1{print "mode: set";next} $NF>0{$NF=1} {print}\' cover_ce.out > cover_ce_coveralls.out'
                                    sh "goveralls -coverprofile=cover_ce_coveralls.out -service=uberjenkins -repotoken=${COVERALLS_TOKEN} || true"
                                }
                            }
                        }

                        stage('EE') {
                            steps {
                                withEnv(["PATH+GO=${env.GOTOOLS}/bin"]) {
                                    githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-ee-unit-tests', description: 'EE Unit Tests Running', status: 'PENDING')

                                    // Build EE coverprofiles
                                    sh "2>&1 go test -timeout=20m -tags ${EE_BUILD_TAG} -coverpkg=./... -coverprofile=cover_ee.out -race -count=1 -v ./... > verbose_ee.out.raw || true"

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

                stage('LiteCore') {
                    stages {
                        stage('against CE') {
                            // TODO: Remove skip
                            when { expression { return false } }
                            steps {
                                echo 'Example of where we could run an alternate version of lite-core unit tests, or against a running SG CE'
                            }
                        }
                        stage('against EE') {
                            steps {
                                githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-litecore-ee', description: 'Running LiteCore Tests', status: 'PENDING')
                                sh 'touch verbose_litecore.out'
                                sh 'touch verbose_litecore-sg_trace.out'
                                script {
                                    withCredentials([sshUserPrivateKey(credentialsId: 'CB SG Robot Github SSH Key', keyFileVariable: 'KEY')]) {
                                        try {
                                            sh 'docker run --rm -v $KEY:/root/.ssh/id_rsa -v `pwd`/sync_gateway_ee-linux:/sync_gateway -v `pwd`/verbose_litecore.out:/output.out -v `pwd`/verbose_litecore-sg_trace.out:/tmp/sglog/sg_trace.log couchbase/sg-test-litecore:latest -legacy-config'
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-litecore-ee', description: 'EE with LiteCore Test Passed', status: 'SUCCESS')
                                        } catch (Exception e) {
                                            githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-litecore-ee', description: 'EE with LiteCore Test Failed', status: 'FAILURE')
                                            // archive verbose test logs in the event of a test failure
                                            archiveArtifacts artifacts: 'verbose_litecore*.out', fingerprint: false
                                            unstable("EE LIteCore Test Failed")
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                stage('Integration') {
                    stages {
                        stage('Master') {
                            when { branch 'master' }
                            steps {
                                echo 'Queueing Integration test for branch "master" ...'
                                // Queues up an async integration test run using default build params (master branch),
                                // but waits up to an hour for batches of PR merges before actually running (via quietPeriod)
                                build job: 'MasterIntegration', quietPeriod: 3600, wait: false
                            }
                        }

                        stage('PR') {
                            // TODO: Remove skip
                            when { expression { return false } }
                            steps {
                                // TODO: Read labels on PR for 'integration-test'
                                // if present, run stage as separate GH status
                                echo 'Example of where we can run integration tests for this commit'
                                gitStatusWrapper(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", description: 'Running EE Integration Test', failureDescription: 'EE Integration Test Failed', gitHubContext: 'sgw-pipeline-integration-ee', successDescription: 'EE Integration Test Passed') {
                                    echo "Waiting for integration test to finish..."
                                    // TODO: add commit parameter
                                    // Block the pipeline, but don't propagate a failure up to the top-level job - rely on gitStatusWrapper letting us know it failed
                                    build job: 'sync-gateway-integration-master', wait: true, propagate: false
                                }
                            }
                        }
                    }
                }
            }
        }
        stage('Benchmarks'){
            when { branch 'master' }
            steps{
                echo 'Queueing Benchmark Run test for branch "master" ...'
                // TODO: Add this back with new system
                // build job: 'sync-gateway-benchmark', parameters: [string(name: 'SG_COMMIT', value: env.SG_COMMIT)], wait: false
            }
        }
    }

    post {
        always {
            // Publish the cobertura formatted test coverage reports into Jenkins
            cobertura autoUpdateHealth: false, onlyStable: false, autoUpdateStability: false, coberturaReportFile: 'reports/coverage-*.xml', conditionalCoverageTargets: '70, 0, 0', failNoReports: false, failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', sourceEncoding: 'ASCII', zoomCoverageChart: false

            // Publish the junit test reports
            junit allowEmptyResults: true, testDataPublishers: [[$class: 'JUnitFlakyTestDataPublisher']], testResults: 'reports/test-*.xml'

            step([$class: 'WsCleanup'])
            sh "go clean -cache"
        }
        unstable {
            // archive non-verbose outputs upon failure for inspection (each verbose output is conditionally archived on stage failure)
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'master') {
                    slackSend color: "danger", message: "Failed tests in master SGW pipeline: ${currentBuild.fullDisplayName}\nAt least one test failed: ${env.BUILD_URL}"
                }
            }
        }
        failure {
            // archive non-verbose outputs upon failure for inspection (each verbose output is conditionally archived on stage failure)
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'master') {
                    slackSend color: "danger", message: "Build failure!!!\nA build failure occurred in the master SGW pipeline: ${currentBuild.fullDisplayName}\nSomething went wrong building: ${env.BUILD_URL}"
                }
            }
        }
        aborted {
            archiveArtifacts excludes: 'verbose_*.out', artifacts: '*.out', fingerprint: false, allowEmptyArchive: true
            script {
                if ("${env.BRANCH_NAME}" == 'master') {
                    slackSend color: "danger", message: "Master SGW pipeline build aborted: ${currentBuild.fullDisplayName}\nCould be due to build timeout: ${env.BUILD_URL}"
                }
            }
        }
    }
}
