import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions
import json

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv,
                              ['JOB_NAME',
                               'params'])
params_str= args['params']
params = json.loads(params_str)
for k,v in params.items():
    val = '"'+v+'"'
    exec(k+ '=%s'%val)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node DB
DB_node1716257255923 = glueContext.create_dynamic_frame.from_options(
                connection_type="{connection_type}",
                connection_options={
                    "useConnectionProperties": "true",
                    "dbtable":"sales_aliases",
                    "connectionName":"{database}"
                },
                transformation_ctx="DB_node1716257255923",
    )
# Script generated for node DB
DB_node1978324683600 = glueContext.create_dynamic_frame.from_options(
                connection_type="{connection_type}",
                connection_options={
                    "useConnectionProperties": "true",
                    "dbtable":"user_aliases",
                    "connectionName":"{database}"
                },
                transformation_ctx="DB_node1978324683600",
    )

# Script generated for node SQL Query 
SqlQuery0 = """
select first(user_aliases.name) as name,
sum(sales_aliases.amount) as total_amount
from user_aliases
inner join sales_aliases
on user_aliases.user_id = sales_aliases.user_id
where sales_aliases.amount > 0
group by name
order by total_amount

"""
SQLTransform_node1797412144043 = sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={
        	"sales_aliases":"DB_node1716257255923",
			"user_aliases":"DB_node1978324683600",
		
        },
        transformation_ctx="SQLTransform_node1797412144043",
        )
# Script generated for node {table_name}
PostgreSQL_node1950576208323 = glueContext.write_dynamic_frame.from_catalog(
            frame=SQLTransform_node1797412144043,
            database="{database}",
            table_name="{table_name}",
            transformation_ctx="PostgreSQL_node1950576208323",
        )
job.commit()
