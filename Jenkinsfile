pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    // We could potentially change this to use a dockerfile agent instead so it can be portable.
    agent { label 'sync-gateway-pipeline-builder' }

    environment {
        GO_VERSION = 'go1.17.5'
        GVM = "/root/.gvm/bin/gvm"
        GO = "/root/.gvm/gos/${GO_VERSION}/bin"
        GOPATH = "${WORKSPACE}/godeps"
        GOTOOLS = "${WORKSPACE}/gotools"
        GOCACHE = "${WORKSPACE}/.gocache"
        BRANCH = "${BRANCH_NAME}"
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
        EE_BUILD_TAG = "cb_sg_enterprise"
        SGW_REPO = "github.com/couchbase/sync_gateway"
        GH_ACCESS_TOKEN_CREDENTIAL = "github_cb-robot-sg_access_token"
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
                }
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
                        withEnv(["PATH+=${GO}", "GOPATH=${GOTOOLS}", "GO111MODULE=off"]) {
                            sh "go env"
                            sh "go version"
                            // unhandled error checker
                            sh 'go get -v -u github.com/kisielk/errcheck'
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
