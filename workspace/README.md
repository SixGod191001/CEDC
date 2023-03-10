# workspace是Glue具体代码的文件夹

workspace
  - glue
    - common  --> 包含将sql转换成脚本的程序，以及相应的工具方法
    - dummy-table-1 --> 表名
      - airflow_dags --> 定义调用该glue job的airflow dag, jenkins pipeline 会在部署时候发布到airflow 集群
      - cfn_template --> 定义创建该glue job的cloudformation, jenkins pipeline 部署时会调用发布
      - script --> pyspark 脚本，发布前该文件夹为空，发布时common里的逻辑会被jenkins调用自动生成script到这个文件夹，然后部署到s3
      - sql --> spark sql 用来生成ETL脚本的
    