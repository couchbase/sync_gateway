pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    // We could potentially change this to use a dockerfile agent instead so it can be portable.
    agent { label 'sync-gateway-pipeline-builder' }

    environment {
        GO_VERSION = 'go1.11.5'
        GVM = "/root/.gvm/bin/gvm"
        GO = "/root/.gvm/gos/${GO_VERSION}/bin"
        GOPATH = "${WORKSPACE}/godeps"
        GOCACHE = "off"
        BRANCH = "${BRANCH_NAME}"
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
        EE_BUILD_TAG = "cb_sg_enterprise"
    }

    stages {
        stage('Setup') {
            parallel {
                stage('Bootstrap') {
                    steps {
                        sh "git rev-parse HEAD > .git/commit-id"
                        script {
                            env.SG_COMMIT = readFile '.git/commit-id'
                            // Set BRANCH variable to target branch if this build is a PR
                            if (env.CHANGE_TARGET) {
                                env.BRANCH = env.CHANGE_TARGET
                            }
                        }

                        // Make a hidden directory to move scm
                        // checkout into, we'll need a bit for later,
                        // but mostly rely on bootstrap.sh to get our code.
                        //
                        // TODO: We could probably change the implicit checkout
                        // to clone directly into a subdirectory instead?
                        sh 'mkdir .scm-checkout'
                        sh 'mv * .scm-checkout/'

                        echo "Bootstrapping commit ${SG_COMMIT}"
                        sh 'cp .scm-checkout/bootstrap.sh .'
                        sh 'chmod +x bootstrap.sh'
                        sh "./bootstrap.sh -p sg-accel -c ${SG_COMMIT}"
                    }
                }

                stage('Go') {
                    stages {
                        stage('Install') {
                            steps {
                                echo 'Installing Go via gvm..'
                                // We'll use Go 1.10.4 to bootstrap compilation of newer Go versions
                                // (because we know this version is installed on the Jenkins node)
                                withEnv(["GOROOT_BOOTSTRAP=/root/.gvm/gos/go1.10.4"]) {
                                    // Use gvm to install the required Go version, if not already
                                    sh "${GVM} install $GO_VERSION"
                                }
                            }
                        }
                        stage('Get Tools') {
                            steps {
                                withEnv(["PATH+=${GO}", "GOPATH=${GOPATH}"]) {
                                    sh "go version"
                                    // cover is used for building HTML reports of coverprofiles
                                    sh 'go get -v -u golang.org/x/tools/cmd/cover'
                                    // goveralls is used to send coverprofiles to coveralls.io
                                    sh 'go get -v -u github.com/mattn/goveralls'
                                    // Jenkins coverage reporting tools
                                    sh 'go get -v -u github.com/axw/gocov/...'
                                    sh 'go get -v -u github.com/AlekSi/gocov-xml'
                                    // Jenkins test reporting tools
                                    sh 'go get -v -u github.com/tebeka/go2xunit'
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('Builds') {
            parallel {
                stage('CE Linux') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            sh "GOOS=linux go build -o sync_gateway_ce-linux -v github.com/couchbase/sync_gateway"
                        }
                    }
                }
                stage('EE Linux') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            sh "GOOS=linux go build -o sync_gateway_ee-linux -tags ${EE_BUILD_TAG} -v github.com/couchbase/sync_gateway"
                        }
                    }
                }
                stage('CE macOS') {
                    // TODO: Remove skip
                    when { expression { return false } }
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            echo 'TODO: figure out why build issues are caused by gosigar'
                            sh "GOOS=darwin go build -o sync_gateway_ce-darwin -v github.com/couchbase/sync_gateway"
                        }
                    }
                }
                stage('EE macOS') {
                    // TODO: Remove skip
                    when { expression { return false } }
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            echo 'TODO: figure out why build issues are caused by gosigar'
                            sh "GOOS=darwin go build -o sync_gateway_ee-darwin -tags ${EE_BUILD_TAG} -v github.com/couchbase/sync_gateway"
                        }
                    }
                }
                stage('CE Windows') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            sh "GOOS=windows go build -o sync_gateway_ce-windows -v github.com/couchbase/sync_gateway"
                        }
                    }
                }
                stage('EE Windows') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            sh "GOOS=windows go build -o sync_gateway_ee-windows -tags ${EE_BUILD_TAG} -v github.com/couchbase/sync_gateway"
                        }
                    }
                }
                stage('Windows Service') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            sh "GOOS=windows go build -o sync_gateway_ce-windows-service -v github.com/couchbase/sync_gateway/service/sg-windows/sg-service"
                        }
                    }
                }
            }
        }

        stage('Checks') {
            parallel {
                stage('gofmt') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            warnError(message: "gofmt failed") {
                                sh "test -z \"\$(gofmt -d -e ${GOPATH}/src/github.com/couchbase/sync_gateway)\""
                            }
                        }
                    }
                }
                stage('go vet') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            warnError(message: "go vet failed") {
                                sh "go vet github.com/couchbase/sync_gateway/..."
                            }
                        }
                    }
                }
                stage('go fix') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            warnError(message: "go fix failed") {
                                sh "test -z \"\$(go tool fix -diff ${GOPATH}/src/github.com/couchbase/sync_gateway)\""
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
                            steps{
                                // Travis-related variables are required as coveralls.io only officially supports a certain set of CI tools.
                                withEnv(["PATH+=${GO}:${GOPATH}/bin", "TRAVIS_BRANCH=${env.BRANCH}", "TRAVIS_PULL_REQUEST=${env.CHANGE_ID}", "TRAVIS_JOB_ID=${env.BUILD_NUMBER}"]) {
                                    // Build CE coverprofiles
                                    sh '2>&1 go test -timeout=20m -coverpkg=github.com/couchbase/sync_gateway/... -coverprofile=cover_ce.out -race -count=1 -v github.com/couchbase/sync_gateway/... > verbose_ce.out || true'

                                    // Print total coverage stats
                                    sh 'go tool cover -func=cover_ce.out | awk \'END{print "Total SG CE Coverage: " $3}\''

                                    sh 'mkdir -p reports'

                                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                                    sh 'gocov convert cover_ce.out | gocov-xml > reports/coverage-ce.xml'

                                    // Generate junit-formatted test report
                                    warnError(message: "At least one CE unit test failed") {
                                        sh 'go2xunit -fail -suite-name-prefix="CE-" -input verbose_ce.out -output reports/test-ce.xml'
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
                                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                                    // Build EE coverprofiles
                                    sh "2>&1 go test -timeout=20m -tags ${EE_BUILD_TAG} -coverpkg=github.com/couchbase/sync_gateway/... -coverprofile=cover_ee.out -race -count=1 -v github.com/couchbase/sync_gateway/... > verbose_ee.out || true"

                                    sh 'go tool cover -func=cover_ee.out | awk \'END{print "Total SG EE Coverage: " $3}\''

                                    sh 'mkdir -p reports'

                                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                                    sh 'gocov convert cover_ee.out | gocov-xml > reports/coverage-ee.xml'

                                    // Generate junit-formatted test report
                                    warnError(message: "At least one EE unit test failed") {
                                        sh 'go2xunit -fail -suite-name-prefix="EE-" -input verbose_ee.out -output reports/test-ee.xml'
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
                                echo 'Example of where we could run lite-core unit tests against a running SG CE'
                                gitStatusWrapper(credentialsId: 'bbrks_uberjenkins_sg_access_token', description: 'Running LiteCore Tests', failureDescription: 'CE with LiteCore Test Failed', gitHubContext: 'sgw-pipeline-litecore-ce', successDescription: 'CE with LiteCore Test Passed') {
                                    echo "..."
                                }
                            }
                        }
                        stage('against EE') {
                            // TODO: Remove skip
                            when { expression { return false } }
                            steps {
                                echo 'Example of where we could run lite-core unit tests against a running SG EE'
                                gitStatusWrapper(credentialsId: 'bbrks_uberjenkins_sg_access_token', description: 'Running LiteCore Tests', failureDescription: 'EE with LiteCore Test Failed', gitHubContext: 'sgw-pipeline-litecore-ee', successDescription: 'EE with LiteCore Test Passed') {
                                    echo "..."
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
                                // Queues up an async integration test run for the master branch, but waits up to an hour for all merges into master before actually running (via quietPeriod)
                                build job: 'sync-gateway-integration-master', quietPeriod: 3600, wait: false
                            }
                        }
                        stage('PR') {
                            // TODO: Remove skip
                            when { expression { return false } }
                            steps {
                                // TODO: Read labels on PR for 'integration-test'
                                // if present, run stage as separate GH status
                                echo 'Example of where we can run integration tests for this commit'
                                gitStatusWrapper(credentialsId: 'bbrks_uberjenkins_sg_access_token', description: 'Running EE Integration Test', failureDescription: 'EE Integration Test Failed', gitHubContext: 'sgw-pipeline-integration-ee', successDescription: 'EE Integration Test Passed') {
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

    }

    post {
        always {
            // Publish the cobertura formatted test coverage reports into Jenkins
            cobertura autoUpdateHealth: false, onlyStable: false, autoUpdateStability: false, coberturaReportFile: 'reports/coverage-*.xml', conditionalCoverageTargets: '70, 0, 0', failNoReports: false, failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', sourceEncoding: 'ASCII', zoomCoverageChart: false

            // Publish the junit test reports
            junit allowEmptyResults: true, testResults: 'reports/test-*.xml'

            // TODO: Might be better to clean the workspace to before a job runs instead
            step([$class: 'WsCleanup'])
        }
    }
}
