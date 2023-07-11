class Constants:
    SQL_GET_JOB_TEMPLATE_NAME = """SELECT DISTINCT job_template_name
from dim_job
where task_name = '{}'"""
    SQL_GET_FAILED_TASKS_NAME = """
    SELECT task_name
FROM (
  SELECT 
    task_name, 
		dag_name,
		status,
    ROW_NUMBER() OVER (PARTITION BY task_name ORDER BY start_date desc) AS row_num
  FROM fact_task_details
) t
WHERE t.row_num = 1 and dag_name = '{}' and status not in ('RUNNING','SUCCESS');
    
    """
###

    SQL_GET_TASKS_NAME = """
SELECT DISTINCT task_name
FROM dim_task
WHERE dag_name='{}'
"""

    SQL_GET_DAG_NAME = """SELECT DISTINCT dag_name FROM dim_task where task_name='{}';"""
    SQL_GET_JOB_RUNID = """SELECT run_id from fact_job_details where job_name='{}' order by job_start_date desc limit 1;"""
    SQL_GET_JOB_NAME = """
SELECT DISTINCT job_name FROM fact_job_details WHERE task_name='{}'
"""
    SQL_GET_JOB_DATE = """
    select job_start_date 
from fact_job_details fjd 
where job_name = '{job_name}'
order by job_start_date desc
limit 1
    """
    SQL_GET_LAST_GLUE_STATE = """select fjd.job_status 
from fact_job_details fjd 
join dim_job dj on fjd.job_name = dj.job_name 
where dj.job_name ='{job_name}'
order by fjd.id desc
limit 1"""
    SQL_GET_JOB_PARAM = """select param_value from dim_job_params djp 
where job_name = '{job_name}'
and param_name='{param_name}'"""
    SQL_GET_JOB_LIST = """SELECT fjd.job_name, fjd.run_id, fjd.last_update_date
FROM fact_job_details fjd
JOIN dim_dag dd ON fjd.dag_name = dd.dag_name
JOIN (
    SELECT f.job_name,MAX(last_update_date) as lst_upd_dt
    FROM fact_job_details f
    WHERE job_name = f.job_name
    group by f.job_name
) m on m.job_name=fjd.job_name and m.lst_upd_dt=fjd.last_update_date
WHERE dd.dag_name = '{dag_name}' 
"""
    SQL_GET_DAG_STATE = """SELECT status FROM fact_dag_details WHERE dag_name = '{dag_name}'
ORDER BY last_update_date desc LIMIT 1"""
    SQL_GET_EMAIL = """select email_header,email_body
from dim_email
WHERE topic = '{topic}' AND email_type = '{email_type}' AND is_active = 'Y';
    """
    GLUE_STARTING = 'STARTING'
    GLUE_RUNNING = 'RUNNING'
    GLUE_STOPPING = 'STOPPING'
    GLUE_STOPPED = 'STOPPED'
    GLUE_SUCCEEDED = 'SUCCEEDED'
    GLUE_FAILED = 'FAILED'
    GLUE_TIMEOUT = 'TIMEOUT'
    GLUE_ERROR = 'ERROR'
    GLUE_WAITING = 'WAITING'
    FORCE_SUCCESS='FORCE_SUCCESS'
