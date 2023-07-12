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
                     choice(name: 'EnvName', choices: ['dev','uat','prod'], description: 'Please Select Environment')
					 booleanParam (name: 'DagValidation', defaultValue: false, description: 'Please perform dag verification')
                     booleanParam (name: 'Airflow', defaultValue: false, description: 'Please Select If need to deploy Airflow Dag')
                     string(name: 'AirflowDagPath', defaultValue: 'airflow_workspace/dags/', description: 'Please Input full path for Airflow Dag')
                     booleanParam (name: 'AirflowScript', defaultValue: false, description: 'Please Select If need to deploy Airflow Script')
                     string(name: 'AirflowScriptPath', defaultValue: 'airflow_workspace/dags/', description: 'Please Input full path for Airflow Script')
                     string(name: 'MetadataDBJsonPath', defaultValue: 'airflow_workspace/metadata_db/DML/dim_data.json', description: 'Please Input full path for Json if you need to deploy Airflow metadata')
					 booleanParam (name: 'dim_dag', defaultValue: false, description: 'Please Select If need to deploy metadata ( Table : dim_dag )')
					 booleanParam (name: 'dim_dag_dependence', defaultValue: false, description: 'Please Select If need to deploy metadata ( Table : dim_dag_dependence )')
					 booleanParam (name: 'dim_email', defaultValue: false, description: 'Please Select If need to deploy metadata ( Table : dim_email )')
					 booleanParam (name: 'dim_task', defaultValue: false, description: 'Please Select If need to deploy metadata ( Table : dim_task )')
					 booleanParam (name: 'dim_job', defaultValue: false, description: 'Please Select If need to deploy metadata ( Table : dim_job )')
					 booleanParam (name: 'dim_job_params', defaultValue: false, description: 'Please Select If need to deploy metadata ( Table : dim_job_params )')

}
	environment {
	      v_user_home="/home/jenkins/workspace"
		  v_dagPath="${v_user_home}/${env.JOB_BASE_NAME}/${params.AirflowDagPath}"
          v_scriptPath="${v_user_home}/${env.JOB_BASE_NAME}/${params.AirflowScriptPath}"
          v_MetadataDBJsonPath="${v_user_home}/${env.JOB_BASE_NAME}/${params.MetadataDBJsonPath}"
		  v_piplinePath="${v_user_home}/${env.JOB_BASE_NAME}"
		  PYTHONPATH="${v_piplinePath}:${PYTHONPATH}"

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

	                    if ( "${params.EnvName}" == 'dev' ) {
	                         v_aws_credentials='test'
	                         v_region="ap-northeast-1"
	                         echo "deploy database on environment: dev"

	                    } else if ( "${params.EnvName}" == 'uat') {
	                         v_aws_credentials='test'
	                         v_region="ap-northeast-1"
	                         echo "deploy database on environment: uat"
	                    } else {
	                        v_aws_credentials='test'
	                        v_region="ap-northeast-1"
	                        echo "deploy database on environment: prod"
	                    }

					 echo "---install python package..."
					 sh """ pip3 install -r ${v_piplinePath}/airflow_workspace/requirements.txt """
					 echo "---install python package done"
	                 echo "============== end set up parameters ========"
	                 current_path = sh (returnStdout: true, script:" pwd ").trim()
	                 echo "${current_path}"
	                 sh """ python --version """
	                 sh """ pip3 list """
                }
            }
        }

		stage('Dag Validation') {
		    when {
                expression { params.DagValidation == true}
            }
			steps {
			    echo "-----Validation Dag is BashOperator"
				sh """cd ${env.v_piplinePath};
				python airflow_workspace/utils/dag_validate.py ${v_dagPath}
				"""
				echo "-----done"
			}
		}

        stage('Deploy Airflow') {
		    when {
                expression { params.Airflow == true}
            }
			steps {
			    script {
				echo "============== start set up parameters ========"
				echo "$v_dagPath*"
				sh 'ls $v_dagPath'
				echo "${params.AirflowDagPath}"
				v_dagPath_params = "${params.AirflowDagPath}"

			    sshPublisher(
                        publishers: [
                            sshPublisherDesc(
                                configName: 'airflow-ec2-server',
                                transfers: [
                                    sshTransfer(
                                        sourceFiles: "${params.AirflowDagPath}*",
                                        removePrefix: "${params.AirflowDagPath}",
                                        remoteDirectory: 'dags'

                                    )
                                ]
                            )
                        ]
                    )
				echo "============== Done deploy airflow dag (copy file to Airflow Server)========"
                }
			}

		}

   	    stage('Deploy Airflow Script') {
		    when {
                expression { params.AirflowScript == true}
            }
			steps {
			    script {
				echo "============== start set up parameters ========"
				v_ScirptPath_params = "${params.AirflowScriptPath}"
			    v_folderNm=v_ScirptPath_params.split("/")[-1]
				v_AirflowScriptS3Path="s3://eliane-bucket/AirflowScript/${v_folderNm}/"
				v_AirflowScriptS3PathArchive="s3://eliane-bucket/AirflowScriptArchive/${v_folderNm}/\$(date '+%Y-%m-%d')/\$(date '+%s')"
				echo "============== start archive S3 folder========"
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}"){
				sh """ aws s3 cp ${v_AirflowScriptS3Path} ${v_AirflowScriptS3PathArchive} --recursive"""}
				echo "Bucket: ${v_AirflowScriptS3Path} has been archived to Bucket: ${v_AirflowScriptS3PathArchive}"
				echo "============== archive S3 folder Done========"
				echo "============== start deploy airflow script (copy file to S3)========"
				withAWS(credentials: "${v_aws_credentials}", region: "${v_region}"){
				sh """ aws s3 cp ${v_scriptPath} ${v_AirflowScriptS3Path} --recursive"""}
				echo "============== Done deploy airflow script (copy file to S3)========"
                }
			}

		}


		stage('Deploy Metadata dim_dag') {
		    when {
                expression { params.dim_dag == true}
            }
			steps {
			   // sh """ pip3 install -r ${v_piplinePath}/airflow_workspace/requirements.txt """
			    echo "-----start deploy metadata DB"
					withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
						sh """cd ${env.v_piplinePath};
					ls
					python airflow_workspace/metadata_db/DML/json_publish_db.py ${v_MetadataDBJsonPath} dim_dag
					"""
					echo "-----done"
				}

			}
		}
		stage('Deploy Metadata dim_dag_dependence') {
		    when {
                expression { params.dim_dag_dependence == true}
            }
			steps {
			 //   sh """ pip3 install -r ${v_piplinePath}/airflow_workspace/requirements.txt """
			    echo "-----start deploy metadata DB"
					withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
						sh """cd ${env.v_piplinePath};
					ls
					python airflow_workspace/metadata_db/DML/json_publish_db.py ${v_MetadataDBJsonPath} dim_dag_dependence
					"""
					echo "-----done"
				}

			}
		}

		stage('Deploy Metadata dim_email') {
		    when {
                expression { params.dim_email == true}
            }
			steps {
			  //  sh """ pip3 install -r ${v_piplinePath}/airflow_workspace/requirements.txt """
			    echo "-----start deploy metadata DB"
					withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
						sh """cd ${env.v_piplinePath};
					ls
					python airflow_workspace/metadata_db/DML/json_publish_db.py ${v_MetadataDBJsonPath} dim_email
					"""
					echo "-----done"
				}

			}
		}
				stage('Deploy Metadata dim_task') {
		    when {
                expression { params.dim_task == true}
            }
			steps {
			 //   sh """ pip3 install -r ${v_piplinePath}/airflow_workspace/requirements.txt """
			    echo "-----start deploy metadata DB"
					withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
						sh """cd ${env.v_piplinePath};
					ls
					python airflow_workspace/metadata_db/DML/json_publish_db.py ${v_MetadataDBJsonPath} dim_task
					"""
					echo "-----done"
				}

			}
		}
		stage('Deploy Metadata dim_job') {
		    when {
                expression { params.dim_job == true}
            }
			steps {
			//    sh """ pip3 install -r ${v_piplinePath}/airflow_workspace/requirements.txt """
			    echo "-----start deploy metadata DB"
					withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
						sh """cd ${env.v_piplinePath};
					ls
					python airflow_workspace/metadata_db/DML/json_publish_db.py ${v_MetadataDBJsonPath} dim_job
					"""
					echo "-----done"
				}

			}
		}

		stage('Deploy Metadata dim_job_params') {
		    when {
                expression { params.dim_job_params == true}
            }
			steps {
			//    sh """ pip3 install -r ${v_piplinePath}/airflow_workspace/requirements.txt """
			    echo "-----start deploy metadata DB"
					withAWS(credentials: "${v_aws_credentials}", region: "${v_region}") {
						sh """cd ${env.v_piplinePath};
					ls
					python airflow_workspace/metadata_db/DML/json_publish_db.py ${v_MetadataDBJsonPath} dim_job_params
					"""
					echo "-----done"
				}

			}
		}


    }
}
