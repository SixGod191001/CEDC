from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

default_args = {
    'owner': 'YANGYANG',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    "etl_dag_1",
    default_args=default_args,
    description='This DAG will execute the glue job and monitoring the status.',
    schedule_interval="00 10,22 * * *",
    catchup=False)

start = BashOperator(
    task_id='start_etl_dag_1',
    bash_command='echo start',
    dag=dag
)

# [START howto_operator_glue]
submit_glue_job = GlueJobOperator(
    task_id="submit_glue_job",
    job_name="devops",
    script_location="s3://jackyyang-aws-training-code/glue-script/devops.py",
    iam_role_name="devops-glue",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},

)

# [END howto_operator_glue]

# GlueJobOperator waits by default, setting as False to test the Sensor below.
submit_glue_job.wait_for_completion = False

# [START howto_sensor_glue]
wait_for_job = GlueJobSensor(
    task_id="wait_for_job",
    job_name="devops",
    # Job ID extracted from previous Glue Job Operator task
    run_id=submit_glue_job.output,
)
# [END howto_sensor_glue]
wait_for_job.poke_interval = 10

chain(
    submit_glue_job,
    wait_for_job,
)
