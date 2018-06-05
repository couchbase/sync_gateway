pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    agent { label 'sync-gateway-ami-builder' }

    environment {
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
        GO_VERSION = 'go1.8.5'
        GOBIN = "/root/.gvm/gos/${GO_VERSION}/bin/go"
        GOPATH = "${WORKSPACE}/godeps"
        SG_REPO = 'bbrks/sync_gateway' // TODO Fixme
    }

    stages {
        stage('Cloning') {
            steps {
                sh "git rev-parse --short HEAD > .git/commit-id"
                script {
                    env.SG_COMMIT = readFile '.git/commit-id'
                }
                
                // Make a hidden directory to move scm
                // code into, we'll need it later.
                sh 'mkdir .scm-checkout'
                sh 'mv * .scm-checkout/'
            }
        }
        stage('Setup Tools') {
            steps {
                sh 'env'
                echo 'Building..'
                sh '${GOBIN} version'
                echo "GOPATH: ${GOPATH}"
                // TODO: get goveralls
            }
        }
        stage('Bootstrap') {
            steps {
                echo "Bootstrapping commit ${SG_COMMIT}"
                sh 'cp .scm-checkout/bootstrap.sh .'
                sh 'chmod +x bootstrap.sh'
                // TODO: running shell scripts taken from the repo seems dangerous? Could this be modified by anyone?
                sh "./bootstrap.sh -c ${SG_COMMIT} -p sg -r ${SG_REPO}"
                sh 'ls -la'
                sh 'ls -la godeps/src/github.com/couchbase/sync_gateway'
                sh 'ls -la godeps/src/github.com/couchbaselabs/sync-gateway-accel'
                // dir "${WORKSPACE}/godeps/src/github.com/couchbase/sync_gateway"
                // sh "echo \"sync_gateway git commit hash: $(git rev-parse HEAD)\""
                // dir "${WORKSPACE}/godeps/src/github.com/couchbaselabs/sync-gateway-accel"
                // sh "sync-gateway-accel git commit hash: $(git rev-parse HEAD)"
                // dir ${WORKSPACE}
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
                sh './test.sh'
            }
        }
        stage('Test with -race') {
            steps {
                echo 'Testing with -race..'
                sh './test.sh -race'
            }
        }
    }

    post {
        always {
            step([$class: 'WsCleanup'])
        }
    }
}
