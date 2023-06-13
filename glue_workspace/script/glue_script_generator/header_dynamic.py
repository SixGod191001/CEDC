import abc
import imp
import os
import re

# tool_path = '../utils'
tool_path = '../../utils'
head_list = []

def get_tool_head(path):
    """
    param
    :path: the location of the tool class
    """
    try:
        file_list = os.listdir(path)
    except:
        file_list = []
        print("This dir is not exits")
    if file_list:
        for file in file_list:
            flag = 0
            file = os.path.join(path, file)
            f_name = file.split('\\')[-1]
            f_name = re.findall(r'(.*?).py', f_name)
            if file.endswith(".py"):
                with open(file, encoding="utf-8") as f:
                    for line in f.readlines():
                        cls_match = re.match(r"class\s(.*?)[\(:]", line)
                        def_match = re.match(r"def\s(.*?)[\(:]", line)
                        if cls_match:
                            flag += 1
                            cls_name = cls_match.group(1)
                            # print(cls_name)
                            try:
                                module = imp.load_source('mycl', file)
                                cls_a = getattr(module, cls_name)
                                if cls_a:
                                    str1 = ''.join(f_name)
                                    str2 = ''.join(cls_name)
                                    cls_head = 'from glue_workspace.script.utils.' + str1 + ' import ' + str2
                                    # print(cls_head)
                                    head_list.append(cls_head)
                            except:
                                pass
                        elif def_match and flag == 0:
                            def_name = def_match.group(1)
                            # print(def_name)
                            try:
                                module = imp.load_source('mydef', file)
                                def_a = getattr(module, def_name)
                                if def_a:
                                    str1 = ''.join(f_name)
                                    str2 = ''.join(def_name)
                                    def_head = 'from glue_workspace.script.utils.' + str1 + ' import ' + str2
                                    # print(def_head)
                                    head_list.append(def_head)
                            except:
                                pass



class Header(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write_header_frame(self):
        pass

class Headertransform(Header):
    def __init__(self, type=None, path=tool_path):
        """
        param
        type: the header which type you want (s3/Postgre)
        path: the location of the tool class
        return: PY_HEAD_STR and PY_TAIL_STR
        """
        self.type = type
        self.path = path


    def write_header_frame(self):
        get_tool_head(self.path)
        self.type = 'postgre'
        t_head = ('\n').join(head_list)

        headert = '''
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv,['JOB_NAME','params'])
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
'''

        if self.type == 's3':
            headerh = '''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
'''
            headerh = headerh + t_head
        elif self.type == 'postgre':
            headerh = '''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
'''
            headerh = headerh + t_head

        PY_HEAD_STR = headerh + headert
        return PY_HEAD_STR


if __name__ == "__main__":
    # get_tool_head(tool_path)
    # print(head_list)

    m = Headertransform(type='s3')
    s= m.write_header_frame()

    print(s)

