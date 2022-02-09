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
        // TODO: EE_BUILD_TAG = "cb_sg_enterprise"
        EE_BUILD_TAG = "NO-cb_sg_enterprise"
        GH_ACCESS_TOKEN_CREDENTIAL = "github_cb-robot-sg_access_token"
        SGW_REPO = "github.com/cbbruno/sync_gateway_mod"
        GO111MODULE = "auto"
        GOPRIVATE = "github.com/couchbaselabs/go-fleecedelta"
    }

    options {
        // This is required if you want to clean before build
        skipDefaultCheckout(true)
    }

    stages {

        stage('SCM') {
            steps {
               // Clean before build
                cleanWs()
                // We need to explicitly checkout from SCM here
                checkout scm 
                
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
                        // withEnv(["PATH+=${GO}", "GOPATH=${GOTOOLS}", "GO111MODULE=off"]) {
                        withEnv(["PATH+=${GO}"]) {
                            sh "go env"
                            sh "go version"
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
        }

        stage('Builds') {
            parallel {
                stage('CE Linux') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            sh "GOOS=linux go build -o sync_gateway_ce-linux -v ${SGW_REPO}"
                        }
                    }
                }
                stage('EE Linux') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            sh "GOOS=linux go build -o sync_gateway_ee-linux -tags ${EE_BUILD_TAG} -v ${SGW_REPO}"
                        }
                    }
                }
                stage('CE macOS') {
                    // TODO: Remove skip
                    when { expression { return false } }
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            echo 'TODO: figure out why build issues are caused by gosigar'
                            sh "GOOS=darwin go build -o sync_gateway_ce-darwin -v ${SGW_REPO}"
                        }
                    }
                }
                stage('EE macOS') {
                    // TODO: Remove skip
                    when { expression { return false } }
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            echo 'TODO: figure out why build issues are caused by gosigar'
                            sh "GOOS=darwin go build -o sync_gateway_ee-darwin -tags ${EE_BUILD_TAG} -v ${SGW_REPO}"
                        }
                    }
                }
                stage('CE Windows') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            sh "GOOS=windows go build -o sync_gateway_ce-windows -v ${SGW_REPO}"
                        }
                    }
                }
                stage('EE Windows') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            sh "GOOS=windows go build -o sync_gateway_ee-windows -tags ${EE_BUILD_TAG} -v ${SGW_REPO}"
                        }
                    }
                }
                stage('Windows Service') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            sh "GOOS=windows go build -o sync_gateway_ce-windows-service -v ${SGW_REPO}/service/sg-windows/sg-service"
                        }
                    }
                }
            }
        }

        stage('Checks') {
            parallel {
                stage('gofmt') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            script {
                                try {
                                    // TODO: githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-gofmt', description: 'Running', status: 'PENDING')
                                    sh "gofmt -d -e . | tee gofmt.out"
                                    sh "test -z \"\$(cat gofmt.out)\""
                                    // TODO: githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-gofmt', description: 'OK', status: 'SUCCESS')
                                } catch (Exception e) {
                                    sh "wc -l < gofmt.out | awk '{printf \$1}' > gofmt.count"
                                    script {
                                        env.GOFMT_COUNT = readFile 'gofmt.count'
                                    }
                                    // TODO: githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-gofmt', description: "found "+env.GOFMT_COUNT+" problems", status: 'FAILURE')
                                    unstable("gofmt failed")
                                }
                            }
                        }
                    }
                }
                stage('go vet') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            warnError(message: "go vet failed") {
                                sh "go vet -tags ${EE_BUILD_TAG} ./..."
                            }
                        }
                    }
                }
                stage('go fix') {
                    steps {
                        withEnv(["PATH+=${GO}"]) {
                            warnError(message: "go fix failed") {
                                sh "go tool fix -diff . | tee gofix.out"
                                sh "test -z \"\$(cat gofix.out)\""
                            }
                        }
                    }
                }
                stage('errcheck') {
                    steps {
                        withEnv(["PATH+=${GO}:${GOTOOLS}/bin"]) {
                            script {
                                try {
                                    // TODO: githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-errcheck', description: 'Running', status: 'PENDING')
                                    sh "errcheck ./... | tee errcheck.out"
                                    sh "test -z \"\$(cat errcheck.out)\""
                                    // TODO: githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-errcheck', description: 'OK', status: 'SUCCESS')
                                } catch (Exception e) {
                                    sh "wc -l < errcheck.out | awk '{printf \$1}' > errcheck.count"
                                    script {
                                        env.ERRCHECK_COUNT = readFile 'errcheck.count'
                                    }
                                    // TODO: githubNotify(credentialsId: "${GH_ACCESS_TOKEN_CREDENTIAL}", context: 'sgw-pipeline-errcheck', description: "found "+env.ERRCHECK_COUNT+" unhandled errors", status: 'FAILURE')
                                    unstable("errcheck failed")
                                }
                            }
                        }
                    }
                }
            }
        }

    }
}
