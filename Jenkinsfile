pipeline {
    agent any
    //agent {
    //    docker {
    //        image 'maven:3-alpine'
    //    }
    //}
    stages {
        stage('Build') {
            steps {
                sh 'cd jdbc-adapter && mvn -Dmaven.test.failure.ignore clean package'
            }
        }
        stage('Integration Test') {
            steps {
                sh './jdbc-adapter/integration-test-data/run_integration_tests.sh'
            }
        }
    }
}
