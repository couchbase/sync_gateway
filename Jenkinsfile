pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    agent { label 'sync-gateway-ami-builder' }

    environment {
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
        GO_VERSION = 'go1.8.5'
        GOBIN = "/root/.gvm/gos/${GO_VERSION}/bin/go"
        GOPATH = "${WORKSPACE}/godeps"
    }

    stages {
        stage('Cloning') {
            steps {
                sh "git rev-parse --short HEAD > .git/commit-id"
                script {
                    env.SG_COMMIT = readFile '.git/commit-id'
                }
                
                // Make a hidden directory to move scm code into,
                // we'll need some bits of it later (bootstrap.sh)
                sh 'ls -la'

                sh 'mkdir .scm-checkout'
                sh 'mv * .scm-checkout/'

                sh 'ls -la'

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
                sh "./bootstrap.sh -c ${SG_COMMIT} -p sg"
                sh 'ls -la'
                sh 'ls -la godeps/src/github.com/couchbase'
                sh 'ls -la godeps/src/github.com/couchbaselabs'
                sh 'ls -la godeps/src/github.com/couchbase/sync_gateway'
                sh 'ls -la godeps/src/github.com/couchbaselabs/sync-gateway-accel'
            }
        }
        stage('Build') {
            steps {
                echo 'Building..'
                sh './build.sh -v'
            }
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
