# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
# MAGIC # Common Utils - Get Project Folder Structure Paths

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.utils import AnalysisException
# MAGIC import json
# MAGIC
# MAGIC
# MAGIC def get_notebook_info():
# MAGIC     try:
# MAGIC         # 获取当前notebook的路径
# MAGIC         notebook_path = (
# MAGIC             dbutils.notebook.entry_point.getDbutils()
# MAGIC             .notebook()
# MAGIC             .getContext()
# MAGIC             .notebookPath()
# MAGIC             .get()
# MAGIC         )
# MAGIC
# MAGIC         # 获取当前用户的用户名
# MAGIC         username = (
# MAGIC             dbutils.notebook.entry_point.getDbutils()
# MAGIC             .notebook()
# MAGIC             .getContext()
# MAGIC             .tags()
# MAGIC             .apply("user")
# MAGIC         )
# MAGIC
# MAGIC         # 获取当前工程的路径
# MAGIC         # 读取repo下的文件应该用file:/Workspace/Repos/<user_folder>/<repo_name>/file
# MAGIC         project_path = f"/Repos/{username}/CEDC/databricks workspace/CEDC_Workspace"
# MAGIC         bronzelayer_path = project_path + "/BronzeLayer"
# MAGIC         sliverlayer_path = project_path + "/SliverLayer"
# MAGIC         goldlayer_path = project_path + "/GoldLayer"
# MAGIC         includes_path = project_path + "/includes"
# MAGIC
# MAGIC         notebook_info = json.dumps(
# MAGIC             {
# MAGIC                 "notebook_path": notebook_path,
# MAGIC                 "project_path": project_path,
# MAGIC                 "username": username,
# MAGIC                 "bronzelayer_path": bronzelayer_path,
# MAGIC                 "sliverlayer_path": sliverlayer_path,
# MAGIC                 "goldlayer_path": goldlayer_path,
# MAGIC                 "includes_path": includes_path,
# MAGIC                 "includes_path": includes_path,
# MAGIC             }
# MAGIC         )
# MAGIC         return notebook_info
# MAGIC     except AnalysisException:
# MAGIC         notebook_info = json.dumps(
# MAGIC             {
# MAGIC                 "notebook_path": None,
# MAGIC                 "project_path": None,
# MAGIC                 "username": None,
# MAGIC                 "bronzelayer_path": None,
# MAGIC                 "sliverlayer_path": None,
# MAGIC                 "goldlayer_path": None,
# MAGIC                 "includes_path": None,
# MAGIC                 "includes_path": None,
# MAGIC             }
# MAGIC         )
# MAGIC         return notebook_info
# MAGIC
# MAGIC
# MAGIC notebook_info = get_notebook_info()
# MAGIC dbutils.notebook.exit(notebook_info)
