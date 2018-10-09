pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    // We could potentially change this to use a dockerfile agent instead so it can be portable.
    agent { label 'sync-gateway-ami-builder' }

    environment {
        GO_VERSION = 'go1.11.1'
        GVM = "/root/.gvm/bin/gvm"
        GO = "/root/.gvm/gos/${GO_VERSION}/bin"
        GOPATH = "${WORKSPACE}/godeps"
        BRANCH = "${BRANCH_NAME}"
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
    }

    stages {
        stage('Cloning') {
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
            }
        }
        stage('Setup Tools') {
            steps {
                echo 'Setting up Go tools..'
                // We'll use Go 1.10.4 to bootstrap compilation of newer Go versions
                // (because we know this version is installed on the Jenkins node)
                withEnv(["GOROOT_BOOTSTRAP=/root/.gvm/gos/go1.10.4"]) {
                    // Use gvm to install the required Go version, if not already
                    sh "${GVM} install $GO_VERSION"
                }
                withEnv(["PATH+=${GO}", "GOPATH=${GOPATH}"]) {
                    sh "go version"
                    // cover is used for building HTML reports of coverprofiles
                    sh 'go get -v -u golang.org/x/tools/cmd/cover'
                    // goveralls is used to send coverprofiles to coveralls.io
                    sh 'go get -v -u github.com/mattn/goveralls'
                    // Jenkins coverage reporting tools
                    // sh 'go get -v -u github.com/axw/gocov/...'
                    // sh 'go get -v -u github.com/AlekSi/gocov-xml'
                }
            }
        }
        stage('Bootstrap') {
            steps {
                echo "Bootstrapping commit ${SG_COMMIT}"
                sh 'cp .scm-checkout/bootstrap.sh .'
                sh 'chmod +x bootstrap.sh'
                sh "./bootstrap.sh -p sg-accel -c ${SG_COMMIT}"
            }
        }
        stage('Build') {
            steps {
                echo 'Building..'
                withEnv(["PATH+=${GO}"]) {
                    sh './build.sh -v'
                }
            }
        }
        stage('Test with coverage') {
            steps {
                echo 'Testing with -cover..'
                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                    // Build public and private coverprofiles (private containing accel code too)
                    sh 'go test -coverpkg=github.com/couchbase/sync_gateway/... -coverprofile=cover_public.out github.com/couchbase/sync_gateway/... github.com/couchbaselabs/sync-gateway-accel/...'
                    sh 'go test -coverpkg=github.com/couchbase/sync_gateway/...,github.com/couchbaselabs/sync-gateway-accel/... -coverprofile=cover_private.out github.com/couchbase/sync_gateway/... github.com/couchbaselabs/sync-gateway-accel/...'

                    // Print total coverage stats
                    sh 'go tool cover -func=cover_public.out | awk \'END{print "Total SG Coverage: " $3}\''
                    sh 'go tool cover -func=cover_private.out | awk \'END{print "Total SG+SGA Coverage: " $3}\''

                    // Publish combined HTML coverage report to Jenkins
                    sh 'mkdir reports'
                    sh 'go tool cover -html=cover_private.out -o reports/coverage.html'
                    publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, includes: 'coverage.html', keepAll: false, reportDir: 'reports', reportFiles: 'coverage.html', reportName: 'Code Coverage', reportTitles: ''])
                }

                // Travis-related variables are required as coveralls only officially supports a certain set of CI tools.
                withEnv(["PATH+=${GO}:${GOPATH}/bin", "TRAVIS_BRANCH=${env.BRANCH}", "TRAVIS_PULL_REQUEST=${env.CHANGE_ID}", "TRAVIS_JOB_ID=${env.BUILD_NUMBER}"]) {
                    // Replace covermode values with set just for coveralls to reduce the variability in reports.
                    sh 'awk \'NR==1{print "mode: set";next} $NF>0{$NF=1} {print}\' cover_public.out > cover_coveralls.out'

                    // Send just the SG coverage report to coveralls.io - **NOT** accel! It will expose the private codebase!!!
                    sh "goveralls -coverprofile=cover_coveralls.out -service=uberjenkins -repotoken=${COVERALLS_TOKEN}"

                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                    // TODO: Requires Cobertura Plugin to be installed on Jenkins first
                    // sh 'gocov convert cover_sg.out | gocov-xml > reports/coverage.xml'
                    // step([$class: 'CoberturaPublisher', coberturaReportFile: 'reports/coverage.xml'])
                }
            }
        }
        stage('Test with race') {
            steps {
                echo 'Testing with -race..'
                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                    sh './test.sh -race -count=1'
                }
            }
        }
    }

    post {
        always {
            // TODO: Might be better to clean the workspace to before a job runs instead
            step([$class: 'WsCleanup'])
        }
    }
}
