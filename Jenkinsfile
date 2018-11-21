pipeline {
    agent {
        docker {
            image 'maven:3-alpine'
        }
    }
    stages {
        stage('Build') {
            steps {
                sh 'cd jdbc-adapter && mvn -Dmaven.test.failure.ignore clean package'
            }
        }
    }
}
