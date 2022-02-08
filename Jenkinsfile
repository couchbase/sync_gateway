pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    // We could potentially change this to use a dockerfile agent instead so it can be portable.
    agent { label 'sync-gateway-pipeline-builder' }

    environment {
        GO_VERSION = '1.17.5'
        GOPATH = "${WORKSPACE}/godeps"
        GOCACHE = "${WORKSPACE}/.gocache"
        BRANCH = "${BRANCH_NAME}"
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
        // EE_BUILD_TAG = "cb_sg_enterprise"
        EE_BUILD_TAG = "NO-cb_sg_enterprise"
        SGW_REPO = "github.com/cbbruno/sync_gateway_mod"
        GH_ACCESS_TOKEN_CREDENTIAL = "github_cb-robot-sg_access_token"
        GO111MODULE = "auto"
        GOPRIVATE = "github.com/couchbaselabs/go-fleecedelta"
    }

    stages {
        stage("Go") {
            steps {
                withEnv(["PATH+=${GOPATH}/bin:${GOROOT}/bin"]) {
                    sh 'go version'
                    sh 'go env'
                }
            }
        }
    }

}
