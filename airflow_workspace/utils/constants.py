class Constants:
    SQL_GET_LAST_GLUE_STATE = """select fjd.job_status 
from fact_job_details fjd 
join dim_job dj on fjd.job_name = dj.job_name 
where dj.job_name ='{job_name}'
order by fjd.id desc
limit 1"""
    SQL_GET_JOB_PARAM = """select param_value from dim_job_params djp 
where job_name = '{job_name}'
and param_name='{param_name}'"""
    SQL_GET_JOB_LIST = """select dj.job_name, max(fjd.run_id) as run_id 
from fact_job_details fjd
join dim_job dj on fjd.job_name = dj.job_name
join dim_dag dd on fjd.dag_name = dd.dag_name
where dd.dag_name  = '{dag_name}' 
group by dj.job_name
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
