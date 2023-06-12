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

# Script generated for node sales_aliases
sales_aliases_node1208539375215 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name="sales_aliases",
    transformation_ctx="sales_aliases_node1208539375215",
)
# Script generated for node user_aliases
user_aliases_node1175018538915 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name="user_aliases",
    transformation_ctx="user_aliases_node1175018538915",
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
SQLTransform_node1579737043400 = sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={
        	"sales_aliases":sales_aliases_node1208539375215,
			"user_aliases":user_aliases_node1175018538915,
		
        },
        transformation_ctx="SQLTransform_node1579737043400",
        )
# Script generated for node S3bucket
repartition_frame = SQLTransform_node1579737043400.repartition(None)
S3bucket_node1041591795909 = glueContext.write_dynamic_frame.from_options(
            frame=repartition_frame,
            connection_type="s3",
            format="csv",
            connection_options = {
                "path": target_path,
                "partitionKeys": [],
            },
            transformation_ctx="S3bucket_node1041591795909",
        )
job.commit()
