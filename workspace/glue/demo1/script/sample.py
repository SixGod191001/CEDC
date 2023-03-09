import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node user
user_node1678341489852 = glueContext.create_dynamic_frame.from_catalog(
    database="devops",
    table_name="user_csv",
    transformation_ctx="user_node1678341489852",
)

# Script generated for node sales
sales_node1678341525471 = glueContext.create_dynamic_frame.from_catalog(
    database="devops",
    table_name="sales_csv",
    transformation_ctx="sales_node1678341525471",
)

# Script generated for node SQL Transform
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
SQLTransform_node1678341562693 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "sales_aliases": sales_node1678341525471,
        "user_aliases": user_node1678341489852,
    },
    transformation_ctx="SQLTransform_node1678341562693",
)

# Script generated for node Target
Target_node1678342936911 = glueContext.write_dynamic_frame.from_options(
    frame=SQLTransform_node1678341562693,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://jackyyang-aws-training-code/output/",
        "partitionKeys": ["user_id"],
    },
    transformation_ctx="Target_node1678342936911",
)

job.commit()
