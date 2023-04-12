def v_FilePath
def v_JobNm
def v_aws_credentials
def v_region
def v_bucket

pipeline {
    agent any;
    parameters { gitParameter name: 'branch', 
                     type: 'PT_BRANCH_TAG',
                     branchFilter: 'origin/(.*)',
                     defaultValue: 'master',
                     selectedValue: 'DEFAULT',
                     sortMode: 'DESCENDING_SMART',
                     description: 'Select your branch or tag.' 
                 string(name: 'JobPath', defaultValue: 'jenkins-framework/glue_script/test_job_001', description: 'Please Input full path') 
                 choice(name: 'EnvName', choices: ['dev','uat','prod'], description: 'Please Select Environment') 
	             booleanParam (name: 'Database', defaultValue: false, description: 'Please Select If need to deploy DB') 
				 booleanParam (name: 'Connection', defaultValue: false, description: 'Please Select If need to deploy Connection')
				 booleanParam (name: 'Cralwer', defaultValue: false, description: 'Please Select If need to deploy Cralwer')
				 booleanParam (name: 'IAM', defaultValue: false, description: 'Please Select If need to deploy IAM Role')
				 booleanParam (name: 'Glue', defaultValue: false, description: 'Please Select If need to deploy Glue')
	}
	environment {
	      v_workspace='/var/lib/jenkins/workspace'
	      v_glueFilePath="/var/lib/jenkins/workspace/${env.JOB_BASE_NAME}/${params.JobPath}"
	      v_cf_path='template'
	      
    }
    stages {
		stage('Get latest Code') {
            steps {
			    script {
				    echo "==============set up parameters:========"
			            v_FilePath = "${params.JobPath}"
			            v_JobNm=v_FilePath.split("/")[-1]
                        v_bucket = sh (returnStdout: true, script:"cat ${env.v_glueFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep Bucket | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
                        v_region = sh (returnStdout: true, script:"cat ${env.v_glueFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep Region | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()

	                if ( "${params.EnvName}" == 'dev' ) {
	                     v_aws_credentials='aws'
	                     echo "deploy database on environment: dev"
	                } else if ( "${params.EnvName}" == 'uat') {
	                     v_aws_credentials='aws'
	                     echo "deploy database on environment: uat"
	                  } else {
	                      v_aws_credentials='aws'
	                      echo "deploy database on environment: prod"
	                 }
                }
                 git branch: "${params.branch}", credentialsId: 'da6de8ca-aade-4b84-9070-294da5819662', url: 'https://gitee.com/liangjian9/shared-info.git'
                 echo "${v_bucket} "
            }
        }        
        stage('Deploy Glue Database') {
		    when {
                expression { params.Database == true}
            }
			steps {
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue database"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-database-template \
                        --template-file ${env.v_glueFilePath}/template_database.yaml \
                        --parameter-overrides file://${env.v_glueFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}                       
                       """
                    sh """ aws s3 cp ${env.v_glueFilePath}/${v_JobNm}.py s3://${v_bucket}/glue-script/ """
				}
			}
		}
        stage('Deploy Glue Connection') {
		    when {
                expression { params.Connection == true}
            }
			steps {
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue connection"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-connection-template \
                        --template-file ${env.v_glueFilePath}/template_connection.yaml \
                        --parameter-overrides file://${env.v_glueFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
				}
			}
		}
        stage('Deploy Glue Cralwer') {
		    when {
                expression { params.Cralwer == true}
            }
			steps {
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue Cralwer"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-cralwer-template \
                        --template-file ${env.v_glueFilePath}/template_cralwer.yaml \
                        --parameter-overrides file://${env.v_glueFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
				}
			}
		}
        stage('Deploy Glue IAM') {
		    when {
                expression { params.IAM == true}
            }
			steps {
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue IAM"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-iam-template \
                        --template-file ${env.v_glueFilePath}/template_iam.yaml \
                        --parameter-overrides file://${env.v_glueFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
				}
			}
		}		
        stage('Deploy Glue') {
		    when {
                expression { params.Glue == true}
            }
			steps {
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy Glue"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-glue-template \
                        --template-file ${env.v_glueFilePath}/template_glue.yaml \
                        --parameter-overrides file://${env.v_glueFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
                    sh """ aws s3 cp ${env.v_glueFilePath}/${v_JobNm}.py s3://${v_bucket}/glue-script/ """  					   
				}
			}
		}		
    }
}
