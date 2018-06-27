pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    // We could potentially change this to use a dockerfile agent instead so it can be portable.
    agent { label 'sync-gateway-ami-builder' }

    environment {
        GO_VERSION = 'go1.8.5'
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
                withEnv(["PATH+=${GO}"]) {
                    sh "go version"
                    sh 'go get -v -u github.com/AlekSi/gocoverutil'
                    sh 'go get -v -u golang.org/x/tools/cmd/cover'
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
                echo 'Testing with coverage..'
                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                    // gocoverutil is required until we upgrade to Go 1.10, and can use -coverprofile with ./...
                    sh 'gocoverutil -coverprofile=cover_sg.out test -covermode=count github.com/couchbase/sync_gateway/...'
                    sh 'gocoverutil -coverprofile=cover_sga.out test -covermode=count github.com/couchbaselabs/sync-gateway-accel/...'

                    sh 'gocoverutil -coverprofile=cover_merged.out merge cover_sg.out cover_sga.out'

                    // Publish combined HTML coverage report
                    sh 'mkdir reports'
                    sh 'go tool cover -html=cover_merged.out -o reports/coverage.html'
                    publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, includes: 'coverage.html', keepAll: false, reportDir: 'reports', reportFiles: 'coverage.html', reportName: 'Code Coverage', reportTitles: ''])
                }

                // Travis-related variables are required as coveralls only officially supports a certain set of CI tools.
                withEnv(["PATH+=${GO}:${GOPATH}/bin", "TRAVIS_BRANCH=${env.BRANCH}", "TRAVIS_PULL_REQUEST=${env.CHANGE_ID}", "TRAVIS_JOB_ID=${env.BUILD_NUMBER}"]) {
                    // Send just the SG coverage report to coveralls.io - **NOT** accel! It will expose the codebase!!!
                    sh "goveralls -coverprofile=cover_sg.out -service=uberjenkins -repotoken=${COVERALLS_TOKEN}"

                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                    // TODO: Requires Cobertura Plugin to be installed on Jenkins first
                    // sh 'gocov convert cover_sg.out | gocov-xml > reports/coverage.xml'
                    // step([$class: 'CoberturaPublisher', coberturaReportFile: 'reports/coverage.xml'])
                }
            }
        }
        stage('Test Race') {
            steps {
                echo 'Testing with -race..'
                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                    sh './test.sh -race'
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
