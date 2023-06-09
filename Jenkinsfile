#!groovy
@Library('lts-basic-pipeline') _

// projName is the directory name for the project on the servers for it's docker/config files
// default values: 
//  registryCredentialsId = "${env.REGISTRY_ID}"
//  registryUri = 'https://registry.lts.harvard.edu'
def endpoints = ['integration']
ltsBasicPipeline.call("etd-alma-drs-holding-service", "ETD", "etd", "10610", endpoints, "lts-etd-alerts")