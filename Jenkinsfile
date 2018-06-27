#!groovy

def workerNode = "devel8"

pipeline {
    agent { label workerNode }

    tools {
        maven "Maven 3"
    }

    triggers {
        pollSCM("H/03 * * * *")
    }

    options {
        timestamps()
    }

    stages {
        stage("clear workspace") {
            steps {
                deleteDir()
                checkout scm
            }
        }

        stage("verify") {
            steps {
                sh "mvn verify pmd:pmd"
                junit "**/target/surefire-reports/TEST-*.xml,**/target/failsafe-reports/TEST-*.xml"
            }
        }

        stage("publish pmd results") {
            steps {
                step([$class: 'hudson.plugins.pmd.PmdPublisher',
                    pattern: '**/target/pmd.xml',
                    unstableTotalAll: "0",
                    failedTotalAll: "0"])
            }
        }

        stage("docker build") {
            when {
                expression {
                    currentBuild.result == null || currentBuild.result == 'SUCCESS'
                }
            }
            steps {
                script {
                    version = env.BRANCH_NAME + '-' + env.BUILD_NUMBER

                    def image = docker.build("docker-io.dbc.dk/rawrepo-content-service-v2:${version}")
                    image.push()

                    if (env.BRANCH_NAME == 'master') {
                        echo 'Pushing build to latest'
                        image.push('latest')
                    }
                }
            }
        }
    }
}