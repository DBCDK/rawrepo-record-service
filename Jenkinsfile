#!groovy

@Library('metascrum')
import dk.dbc.metascrum.jenkins.Maven
// Defined in https://github.com/DBCDK/metascrum-pipeline-library/blob/master/src/dk/dbc/metascrum/jenkins/Maven.groovy
def workerNode = "devel10"

void notifyOfBuildStatus(final String buildStatus) {
    final String subject = "${buildStatus}: ${env.JOB_NAME} #${env.BUILD_NUMBER}"
    final String details = """<p> Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
    <p>Check console output at &QUOT;<a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a>&QUOT;</p>"""
    emailext(
            subject: "$subject",
            body: "$details", attachLog: true, compressLog: false,
            mimeType: "text/html",
            recipientProviders: [[$class: "CulpritsRecipientProvider"]]
    )
}

pipeline {
    agent { label workerNode }

    tools {
        maven "Maven 3"
        jdk 'jdk11'
    }

    triggers {
        pollSCM("H/03 * * * *")
        upstream(upstreamProjects: "Docker-payara5-bump-trigger",
                threshold: hudson.model.Result.SUCCESS)
    }

    options {
        timestamps()
    }

    environment {
        GITLAB_PRIVATE_TOKEN = credentials("metascrum-gitlab-api-token")
        DOCKER_IMAGE_NAME = "docker-metascrum.artifacts.dbccloud.dk/rawrepo-record-service"
        DOCKER_IMAGE_VERSION = "${env.BRANCH_NAME}-${env.BUILD_NUMBER}"
        DOCKER_IMAGE_DIT_VERSION = "DIT-${env.BUILD_NUMBER}"
    }

    stages {
        stage("Clean Workspace") {
            steps {
                deleteDir()
                checkout scm
            }
        }

        stage("Verify") {
            script {
                Maven.verify(this)
            }
        }

        stage("Publish PMD Results") {
            steps {
                step([$class          : 'hudson.plugins.pmd.PmdPublisher',
                      pattern         : '**/target/pmd.xml',
                      unstableTotalAll: "0",
                      failedTotalAll  : "0"])
            }
        }

        stage("Docker build") {
            when {
                expression {
                    currentBuild.result == null || currentBuild.result == 'SUCCESS'
                }
            }
            steps {
                script {
                    def image = docker.build("${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}")
                    image.push()

                    if (env.BRANCH_NAME == 'master') {
                        sh """
                            docker tag ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} ${DOCKER_IMAGE_NAME}:${
                            DOCKER_IMAGE_DIT_VERSION
                        }
                            docker push ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_DIT_VERSION}
                        """
                    }
                }
            }
        }

        stage("Bump deploy version") {
            agent {
                docker {
                    label workerNode
                    image "docker.dbc.dk/build-env:latest"
                    alwaysPull true
                }
            }

            when {
                expression {
                    currentBuild.result == null || currentBuild.result == 'SUCCESS'
                }
            }
            steps {
                script {

                    if (env.BRANCH_NAME == 'master') {
                        sh """
                            set-new-version rawrepo-record-service.yml ${env.GITLAB_PRIVATE_TOKEN} metascrum/rr-record-service-deploy ${DOCKER_IMAGE_DIT_VERSION} -b metascrum-staging
                            set-new-version rawrepo-record-service.yml ${env.GITLAB_PRIVATE_TOKEN} metascrum/rr-record-service-deploy ${DOCKER_IMAGE_DIT_VERSION} -b fbstest
                            set-new-version rawrepo-record-service.yml ${env.GITLAB_PRIVATE_TOKEN} metascrum/rr-record-service-deploy ${DOCKER_IMAGE_DIT_VERSION} -b basismig

                            set-new-version services/rawrepo-record-service.yml ${env.GITLAB_PRIVATE_TOKEN} metascrum/dit-gitops-secrets ${DOCKER_IMAGE_DIT_VERSION} -b master
                        """
                    }
                }
            }
        }

    }

    post {
        unstable {
            notifyOfBuildStatus("Build became unstable")
        }
        failure {
            notifyOfBuildStatus("Build failed")
        }
    }
}
