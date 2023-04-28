# workspace是Glue具体代码的文件夹

workspace
  - glue
    - job --> job名称
      - airflow_dags  --> 定义调用该glue job的airflow dag, jenkins pipeline 会在部署时候发布到airflow 集群
        - dag.py      --> dag
        - params.json --> airflow用到的参数，会存到metadata db
      - etl.sql       --> spark sql 用来生成ETL脚本的
      - params.json   --> glue job用到的参数，会存到metadata db
    
