pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    agent { label 'sync-gateway-ami-builder' }

    options {
        // Let bootstrap.sh handle the checkout
        skipDefaultCheckout true
    }

    environment {
        COVERALLS_TOKEN = credentials('SG_COVERALLS_TOKEN')
    }

    stages {
        stage('Setup Tools') {
            steps {
                sh 'env'
                echo 'Building..'
            }
        }
        stage('Bootstrap') {
            steps {
                echo 'Bootstrapping..'
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
            }
        }
        stage('Test with -race') {
            steps {
                echo 'Testing with -race..'
            }
        }
    }

    post {
        always {
            step([$class: 'WsCleanup'])
        }
    }
}
