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

# Script generated for node user_aliases
user_aliases_node1130859532119 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name="user_aliases",
    transformation_ctx="user_aliases_node1130859532119",
)
# Script generated for node sales_aliases
sales_aliases_node1957625348517 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name="sales_aliases",
    transformation_ctx="sales_aliases_node1957625348517",
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
SQLTransform_node1156696446694 = sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={
        	"user_aliases":user_aliases_node1130859532119,
			"sales_aliases":sales_aliases_node1957625348517,
		
        },
        transformation_ctx="SQLTransform_node1156696446694",
        )
# Script generated for node {table_name}
PostgreSQL_node1491699610557 = glueContext.write_dynamic_frame.from_catalog(
            frame=SQLTransform_node1156696446694,
            database={database},
            table_name={table_name},
            transformation_ctx="PostgreSQL_node1491699610557",
        )
job.commit()
