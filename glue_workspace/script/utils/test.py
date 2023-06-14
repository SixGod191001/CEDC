import re

string = 'connection_type="{connection_type}",connection_options={"useConnectionProperties":"true","dbtable":"sales_aliases","connectionName":"{database}"},transformation_ctx="DB_node1599249796419"'

# 使用正则表达式将大括号内的内容替换为占位符
pattern = r'\{[^{}]+\}'
replaced_string = re.sub(pattern, lambda m: m.group().replace(',', ';'), string)
#print(replaced_string)

# 使用逗号分隔字符串
result = replaced_string.split(',')

print(result[1])