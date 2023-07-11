from airflow_workspace.utils.postgre_handler import PostgresHandler

# tag= "'sales','dalian','department1'"
def get_dag_list(tag):
    """
    :param tag: the tag of dag, should be a list.
    :return: The list of dags
    sample result :
    dag_list = [{'dag_name': 'a', 'flag': 'success', 'tag': 'department1,dalian,sales'},
                 {'dag_name': 'b', 'flag': 'failed', 'tag': 'department2,beijing,sales'}
                 ]
    """
    dag_list_sql = f"""
        select dag_name,status as flag,tag
        from 
        (select d.dag_name,d.tag,f.status ,execution_date,
        row_number () over (partition by d.dag_name order by f.execution_date desc) idx
        from 
        public.dim_dag d
        left join public.fact_dag_details f on f.dag_name =d.dag_name 
        where d.tag in ({tag})
        )a where idx=1
        """
    dag_list = PostgresHandler().get_record(dag_list_sql)
    return dag_list

def get_dag_chain(tag):
    """
    :param tag: the tag of dag, should be a list.
    :return: [[parent dag;dag],[parent dag;dag]]
    sample result :
    lst = [["a", "b"], ["a", "c"], ["b", "d"], ["c", "e"], ["d", "f"], ["e", "f"], ["g", "h"], [None, "w"]]
    """
    query_dag_chains_sql = f"""
    WITH RECURSIVE base AS
    (
    select m.dag_name,dependency_dag_name  from public.dim_dag_dependence m
    inner join public.dim_dag d on m.dag_name =d.dag_name 
    and d.tag in  ({tag}) 
     union
        SELECT child.dag_name,child.dependency_dag_name 
        FROM public.dim_dag_dependence child
        inner join base on  child.dependency_dag_name  = base.dag_name
    )

    select dependency_dag_name||';'||dag_name as dag_chain from base
        """
    lst = []
    dag_chain = PostgresHandler().get_record(query_dag_chains_sql)
    for i in [i['dag_chain'] for i in dag_chain]:
        ls = i.split(";")
        lst.append(ls)
    return lst


