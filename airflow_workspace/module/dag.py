from airflow_workspace.utils.constants import Constants
from airflow_workspace.utils.postgre_handler import PostgresHandler


class process_dag():
    def __init__(self):
        self.dag_id=''
        self.task_name=''

    def start_dag(self,event):
        self.dag_id = event['dag_id']
        self.task_name = event['task_name']
        query_sql = f"""
        select  dag_name from public.fact_dag_details
        where dag_name='{self.dag_id}' and lower(status)='running'
        """
        running_dag = PostgresHandler().get_record(query_sql)
        insert_sql = f""" insert into fact_dag_details 
                                          (dag_name,execution_date,start_date,end_date,run_id,status,run_type,last_scheduling_decision,insert_date,last_update_date)
                                          values('{self.dag_id}',
                                              current_timestamp ,
                                              current_timestamp  ,
                                              null ,
                                              null ,
                                              '{Constants.GLUE_RUNNING}',
                                              null  ,
                                              null ,
                                              current_timestamp ,
                                              current_timestamp  
                                             )
                                      """
        update_sql = f""" update fact_dag_details 
        set end_date=current_timestamp,
        duration=null,
        status='{Constants.FORCE_SUCCESS}'
        last_update_date=current_timestamp
        where dag_name='{self.dag_id}' and lower(status)='running'
                                      """
        if running_dag is not None or running_dag != []:
            PostgresHandler().execute_sql(insert_sql)
        else:
            PostgresHandler().execute_sql(update_sql)
            PostgresHandler().execute_sql(insert_sql)