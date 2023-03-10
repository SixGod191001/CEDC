import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

SqlQuery0 = '''
select first(user_aliases.name) as name, 
sum(sales_aliases.amount) as total_amount 
from user_aliases 
inner join sales_aliases
on user_aliases.user_id = sales_aliases.user_id
where sales_aliases.amount > 0
group by name
order by total_amount


'''

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "devops", table_name = "user_csv", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "devops", table_name = "user_csv", transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [database = "devops", table_name = "sales_csv", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "devops", table_name = "sales_csv", transformation_ctx = "DataSource1")
## @type: SqlCode
## @args: [sqlAliases = {"sales_aliases": DataSource1, "user_aliases": DataSource0}, sqlName = SqlQuery0, transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = DataSource1,DataSource0]
Transform0 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"sales_aliases": DataSource1, "user_aliases": DataSource0}, transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://jackyyang-aws-training-code/output/", "partitionKeys": ["user_id"]}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "csv", connection_options = {"path": "s3://jackyyang-aws-training-code/output/", "partitionKeys": ["user_id"]}, transformation_ctx = "DataSink0")
job.commit()