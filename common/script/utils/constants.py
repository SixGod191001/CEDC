
class Constants:

    PY_HEAD_STR = '''import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv,
                              ['JOB_NAME',
                               'database',
                               'sql_path',
                               'target_path',
                               'out_py_path'])
database= args['database']
sql_path= args['sql_path']
target_path= args['target_path']
out_py_path= args['out_py_path']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
'''
    PY_TAIL_STR = 'job.commit()'