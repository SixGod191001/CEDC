from airflow_workspace.config.constants import Constants
from airflow_workspace.utils.postgre_handler import PostgresHandler
from airflow.exceptions import AirflowFailException

class process_dag():
    def __init__(self):
        self.dag_id=''

    def start_dag(self,event):
        self.dag_id = event['dag_id']
        query_sql = f"""
        select  dag_name from public.fact_dag_details
        where dag_name='{self.dag_id}' and lower(status)='running'
        """
        running_dag = PostgresHandler().execute_select(query_sql)
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
        status='{Constants.FORCE_SUCCESS}',
        last_update_date=current_timestamp
        where dag_name='{self.dag_id}' and lower(status)='running'
                                      """
        if running_dag is not None or running_dag != []:
            PostgresHandler().execute_sql(update_sql)
            PostgresHandler().execute_sql(insert_sql)
        else:
            PostgresHandler().execute_sql(insert_sql)
    def __get_dag_status(self):
        print(f"dag id: {self.dag_id}")
        query_sql = f"""
        with base as (
        select  dag_name,
        status,
        row_number () over(partition by dag_name order by execution_date desc ) row_num
        from 
        public.fact_dag_details 
        )
        select * from base where row_num=1  and dag_name='{self.dag_id}'
        """
        result = PostgresHandler().execute_select(query_sql)
        print(f"query result:{result}")
        print(f"query result[0]:{result[0]}")
        status = result[0]['status']
        print(f"query status:{status}")
        return status

    def dag_check(self,event):
        self.dag_id = event['dag_id']
        print(f"dag id: {self.dag_id}")
        status = self.__get_dag_status()
        if status.lower() != 'success':
            raise AirflowFailException(
                f'dag 执行失败')


if __name__ == "__main__":
    event= {"dag_id": "dag_cedc_sales_landing"}
    process_dag().dag_check(event)
