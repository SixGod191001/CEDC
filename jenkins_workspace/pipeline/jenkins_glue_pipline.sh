pipeline {
    agent {
            label 'ecs'
    }
    parameters { gitParameter name: 'branch', 
                     type: 'PT_BRANCH_TAG',
                     branchFilter: 'origin/(.*)',
                     defaultValue: 'main',
                     selectedValue: 'DEFAULT',
                     sortMode: 'DESCENDING_SMART',
                     description: 'Select your branch or tag.' 
                     string(name: 'JobPath', defaultValue: 'glue_workspace/glue_job/test_glue/glue-catalog', description: 'Please Input full path')
                     choice(name: 'EnvName', choices: ['dev','uat','prod'], description: 'Please Select Environment')
                     booleanParam (name: 'IAM', defaultValue: false, description: 'Please Select If need to deploy IAM Role')
                     booleanParam (name: 'Database', defaultValue: false, description: 'Please Select If need to deploy DB')
                     booleanParam (name: 'Connection', defaultValue: false, description: 'Please Select If need to deploy Connection')
                     booleanParam (name: 'Cralwer', defaultValue: false, description: 'Please Select If need to deploy Cralwer')
                     booleanParam (name: 'GlueTemp', defaultValue: false, description: 'Please Select If need to deploy GlueTemp')
                     booleanParam (name: 'GlueSpark', defaultValue: false, description: 'Please Select If need to deploy GlueSpark')
                     choice(name: 'TargetType', choices: ['PostgreSQL','CSV','MySQL'], description: 'Please Select TargetType')

	}
	environment {
	      v_user_home="/home/jenkins/workspace"
	      v_jobFilePath="${v_user_home}/${env.JOB_BASE_NAME}/${params.JobPath}"
		  v_piplinePath="${v_user_home}/${env.JOB_BASE_NAME}"
		  v_genetatorGlue_cmd="${v_user_home}/${env.JOB_BASE_NAME}/glue_workspace/script/glue_script_generator"
		  v_template_path="${v_user_home}/${env.JOB_BASE_NAME}/glue_workspace/template/cfn-template-glue"
          v_templatePath_Glue="${v_user_home}/${env.JOB_BASE_NAME}/glue_workspace/template/cfn-template-glue-job"
		  PYTHONPATH="${v_piplinePath}:${PYTHONPATH}"
		  v_temp_glue_cmd="glue_workspace/script/glue_script_generator"
		  v_glueScriptPath_local="/home/jenkins/common/script"
		  v_cf_path="template"
    }
    stages {
		stage('Get latest Code') {
            steps {
                 git branch: "${params.branch}", credentialsId: 'Github-SixGod', url: 'https://github.com/SixGod191001/CEDC.git'

            }
        }

        stage('Deploy Environmental Preparation') {
            steps {
			    script {
				    echo "============== start set up parameters ========"
				        v_filePath = "${params.JobPath}"
			            v_folderNm=v_filePath.split("/")[-1]
						v_bucket = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w BucketName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
                        v_region = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w Region | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
                        v_type = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w Type | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
						v_dbtype = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w DBType | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()

	                    if ( "${params.EnvName}" == 'dev' ) {
	                         v_aws_credentials='aws_s3'
	                         echo "deploy database on environment: dev"
	                    } else if ( "${params.EnvName}" == 'uat') {
	                         v_aws_credentials='awscli'
	                         echo "deploy database on environment: uat"
	                    } else {
	                        v_aws_credentials='awscli'
	                        echo "deploy database on environment: prod"
	                    }
						echo "BucketNm: ${v_bucket}"
	                    echo "RegionNm: ${v_region}"
                        echo "Type: ${v_type}"
						echo "DBType: ${v_dbtype}"
						echo "---install python package..."
						sh """ pip3 install -r ${v_piplinePath}/glue_workspace/requirements.txt """
						echo "---install python package done"
	                 echo "============== end set up parameters ========"
	                 current_path = sh (returnStdout: true, script:" pwd ").trim()
	                 echo "${current_path}"
	                 sh """ cat ${env.v_jobFilePath}/params.json """
	                 sh """ python --version """
	                 sh """ pip3 list """
                }
            }
        }
       stage('Deploy Glue IAM') {
		    when {
                expression { params.IAM == true}
            }
			steps {
			    script {
			     v_iam_nm = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -i iamName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
                 v_template="${env.v_template_path}/template_iam.yaml"
			    }
			    echo "using glue iam cloudformation template:${v_template} "
                echo "-----start iam deploy"
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue IAM"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-iam-template-${v_iam_nm} \
                        --capabilities CAPABILITY_NAMED_IAM \
                        --template-file ${v_template} \
                        --parameter-overrides file://${env.v_jobFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
				}
			}
		}
        stage('Deploy Glue Database') {
		    when {
                expression { params.Database == true}
            }
			steps {
			    script {
			     v_database = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w DBName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
                 v_template="${env.v_template_path}/template_database.yaml"
			    }
			    echo "using glue database cloudformation template:${v_template} "
                echo "-----start database deploy"
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue database"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-database-template-${v_database} \
                        --template-file ${v_template} \
                        --parameter-overrides file://${env.v_jobFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
			    }
			}
        }
        stage('Deploy Glue Connection') {
		    when {
                expression { params.Connection == true}
            }
			steps {
			    script {
						if ( "${v_type}" == 'db' ) {
						     v_conn_nm = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -i ${v_dbtype}ConnName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
						     v_template="${env.v_template_path}/template_connection_${v_dbtype}.yaml"

	                    } else if ( "${params.EnvName}" == 'file') {
	                         v_conn_nm = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -i S3ConnName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
	                         v_template="${env.v_template_path}/template_connection_network_s3.yaml"

	                    } else {
	                        v_conn_nm='connection'
							v_template="${env.v_template_path}/template_connection.yaml"
	                    }
			    }

			    echo "using glue connection cloudformation template : ${v_template} "
                echo "-----start connection deploy"
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue database"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-connection-template-${v_conn_nm} \
                        --template-file ${v_template} \
                        --parameter-overrides file://${env.v_jobFilePath}/params.json \
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
			    script {
						if ( "${v_type}" == 'db' ) {
						     v_cralwer_nm = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -i ${v_dbtype}CrawlerName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
	                         v_template="${env.v_template_path}/template_crawler_${v_dbtype}.yaml"

	                    } else if ( "${params.EnvName}" == 'file') {
	                         v_cralwer_nm = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -i S3CrawlerName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
	                         v_template="${env.v_template_path}/template_crawler_s3.yaml"

	                    } else {
	                        v_cralwer_nm='cralwer'
	                        v_template="${env.v_template_path}/template_crawler.yaml"
	                    }
			    }
			    echo "using glue crawler cloudformation template : ${v_template} "
				echo "-----start cralwer deploy"
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy glue Cralwer"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-cralwer-template-${v_cralwer_nm} \
                        --template-file ${v_template} \
                        --parameter-overrides file://${env.v_jobFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
				}
			}
		}
        stage('Deploy GlueTemp') {
		    when {
                expression { params.GlueTemp == true}
            }
			steps {
			    script {
			     v_glueJobNm = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w GlueName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
                 v_template="${env.v_templatePath_Glue}/template_glue.yaml"
			    }
			    echo "using glue cloudformation template:${v_template} "
			    echo "-----start deploy glue template"
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
					sh """echo Start to deploy Glue Template"""
					sh """
                        aws cloudformation deploy \
                        --stack-name devops-glue-template-${v_glueJobNm} \
                        --template-file ${v_template} \
                        --parameter-overrides file://${env.v_jobFilePath}/params.json \
                        --s3-bucket ${v_bucket} \
                        --s3-prefix ${env.v_cf_path} \
                        --region ${v_region}
                       """
				echo "-----done"
				}
			}
		}
		    stage('Deploy GlueSpark') {
        		    when {
                        expression { params.GlueSpark == true}
                    }
        			steps {
        			    script {
        			     v_glueJobNm = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w GlueName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
        			     v_glueDB = sh (returnStdout: true, script:"cat ${env.v_jobFilePath}/params.json | sed ':a;N;\$!ba;s/\\r\\n/\\n/g' | grep -w DBName | awk -F ':' '{print \$2'} | sed 's/\"//g' | awk -F ',' '{print \$1'} | sed s/[[:space:]]//g ").trim()
        			     v_glueScriptS3Path = "s3://${v_bucket}/glue-script/"
                         v_sqlFileNm = sh (returnStdout: true, script:"ls ${env.v_jobFilePath} | grep -i .sql  ").trim()
                         v_sqlFileFullPath="${env.v_jobFilePath}"
                         v_s3output_path = "s3://${v_bucket}/output/${v_glueJobNm}"
                         v_glueScriptFullNm_local="${v_glueScriptPath_local}/"
                         v_glueTargetType="${params.TargetType}"
        			    }
        				echo "-----make local script path "
        				sh """ if [ ! -d ${v_glueScriptPath_local} ]; then
                                  mkdir -p ${v_glueScriptPath_local}
                                fi
                            """
        			    echo "Glue SQL Scrpt Path: ${v_sqlFileFullPath}"
        			    echo "Glue Script S3 Path: ${v_glueScriptS3Path}"
        			    echo "-----start deploy glue"
        				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
                        echo "-----generate glue script using pyspark"
                        echo "export PYTHONPATH=${env.v_piplinePath}:${PYTHONPATH}"
        				sh """cd ${env.v_piplinePath}/glue_workspace/script/glue_script_generator/;
                            ls
        				      python glue_script_generator.py ${v_sqlFileFullPath} ${v_glueScriptFullNm_local} ${v_glueTargetType}
        				  """

        				sh """
        				cd ${v_glueScriptFullNm_local}
        				ls
        				"""

        				echo "-----upload glue script to s3"
        				sh """ aws s3 cp ${v_glueScriptFullNm_local} ${v_glueScriptS3Path} --recursive"""

        				}
        			}
        		}
    }
}
