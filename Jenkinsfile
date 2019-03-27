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

                stage(' ') {
                    stages {
                        stage('Go') {
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
                        stage('Tools') {
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
                                    // sh 'go get -v -u github.com/jstemmer/go-junit-report'
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('Build') {
            parallel {
                stage('gofmt check') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                            sh "test -z \"\$(gofmt -d -e ${GOPATH}/src/github.com/couchbase/sync_gateway)\""
                        }
                    }
                }
                stage('Windows Service') {
                    steps {
                        withEnv(["GOOS=windows", "PATH+=${GO}:${GOPATH}/bin"]) {
                            sh 'go build -v github.com/couchbase/sync_gateway/service/sg-windows/sg-service'
                            sh 'go build -v github.com/couchbase/sync_gateway/service/sg-windows/sg-accel-service'
                        }
                    }
                }
                stage(' ') {
                    stages {
                        stage('CE') {
                            steps {
                                withEnv(["SG_EDITION=CE", "PATH+=${GO}:${GOPATH}/bin"]) {
                                    sh './build.sh -v'
                                }
                            }
                        }
                        stage('EE') {
                            steps {
                                withEnv(["SG_EDITION=EE", "PATH+=${GO}:${GOPATH}/bin"]) {
                                    sh './build.sh -v'
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('Test') {
            parallel {
                stage(' ') {
                    stages {
                        stage('CE -cover') {
                            steps{
                                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                                    // Build public and private coverprofiles (private containing accel code too)
                                    sh 'go test -timeout=20m -coverpkg=github.com/couchbase/sync_gateway/... -coverprofile=cover_ce_public.out github.com/couchbase/sync_gateway/... github.com/couchbaselabs/sync-gateway-accel/...'
                                    sh 'go test -timeout=20m -coverpkg=github.com/couchbase/sync_gateway/...,github.com/couchbaselabs/sync-gateway-accel/... -coverprofile=cover_ce_private.out github.com/couchbase/sync_gateway/... github.com/couchbaselabs/sync-gateway-accel/...'

                                    // Print total coverage stats
                                    sh 'go tool cover -func=cover_ce_public.out | awk \'END{print "Total SG CE Coverage: " $3}\''
                                    sh 'go tool cover -func=cover_ce_private.out | awk \'END{print "Total SG CE+SGA Coverage: " $3}\''

                                    sh 'mkdir -p reports'

                                    // Generate junit-formatted test report
                                    // sh 'cat test_ce.out | go-junit-report > reports/test-ce.xml'

                                    // Generate HTML coverage report
                                    sh 'go tool cover -html=cover_ce_private.out -o reports/coverage-ce.html'

                                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                                    sh 'gocov convert cover_ce_private.out | gocov-xml > reports/coverage-ce.xml'
                                }
                            }
                        }

                        stage('CE Coveralls') {
                            steps {
                                // Travis-related variables are required as coveralls only officially supports a certain set of CI tools.
                                withEnv(["PATH+=${GO}:${GOPATH}/bin", "TRAVIS_BRANCH=${env.BRANCH}", "TRAVIS_PULL_REQUEST=${env.CHANGE_ID}", "TRAVIS_JOB_ID=${env.BUILD_NUMBER}"]) {
                                    // Replace covermode values with set just for coveralls to reduce the variability in reports.
                                    sh 'awk \'NR==1{print "mode: set";next} $NF>0{$NF=1} {print}\' cover_ce_public.out > cover_ce_coveralls.out'

                                    // Send just the SG coverage report to coveralls.io - **NOT** accel! It will expose the private codebase!!!
                                    sh "goveralls -coverprofile=cover_ce_coveralls.out -service=uberjenkins -repotoken=${COVERALLS_TOKEN}"
                                }
                            }
                        }

                        stage('EE -cover') {
                            steps {
                                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                                    sh "go test -timeout=20m -tags ${EE_BUILD_TAG} -coverpkg=github.com/couchbase/sync_gateway/...,github.com/couchbaselabs/sync-gateway-accel/... -coverprofile=cover_ee_private.out github.com/couchbase/sync_gateway/... github.com/couchbaselabs/sync-gateway-accel/..."
                                    sh 'go tool cover -func=cover_ee_private.out | awk \'END{print "Total SG EE+SGA Coverage: " $3}\''

                                    sh 'mkdir -p reports'

                                    // Generate junit-formatted test report
                                    // sh 'cat test_ee.out | go-junit-report > reports/test-ee.xml'

                                    sh 'go tool cover -html=cover_ee_private.out -o reports/coverage-ee.html'

                                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                                    sh 'gocov convert cover_ee_private.out | gocov-xml > reports/coverage-ee.xml'
                                }
                            }
                        }

                        stage('CE -race') {
                            steps {
                                echo 'Testing with -race..'
                                withEnv(["SG_EDITION=CE", "PATH+=${GO}:${GOPATH}/bin"]) {
                                    sh './test.sh -race -count=1'
                                }
                            }
                        }

                        stage('EE -race') {
                            steps {
                                echo 'Testing with -race..'
                                withEnv(["SG_EDITION=EE", "PATH+=${GO}:${GOPATH}/bin"]) {
                                    sh './test.sh -race -count=1'
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
            // Publish the HTML test coverage reports we generated
            publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, includes: 'coverage-*.html', keepAll: false, reportDir: 'reports', reportFiles: '*.html', reportName: 'Code Coverage', reportTitles: ''])

            // Publish the cobertura formatted test coverage reports into Jenkins
            cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'reports/coverage-*.xml', conditionalCoverageTargets: '70, 0, 0', failNoReports: false, failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', sourceEncoding: 'ASCII', zoomCoverageChart: false

            // Publish the junit test reports
            // junit allowEmptyResults: true, testResults: 'reports/test-*.xml'

            // TODO: Might be better to clean the workspace to before a job runs instead
            step([$class: 'WsCleanup'])
        }
    }
}
