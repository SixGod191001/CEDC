# AirflowDagHandler 文档

`AirflowDagHandler` 是一个用于处理 Airflow DAG 的辅助类，提供了通过数据库和 API 查询 DAG 相关信息的功能。

## 构造函数

```python

def __init__(self, base_url):
```

### 参数

* `base_url` (str): Airflow Web 服务器的 URL。

***

## 方法

```python

def get_dag_info(self, dag_id):`
```

通过数据库查询获取指定 DAG 的维度信息。

### 参数

* `dag_id` (str): 要查询的 DAG 的 ID。

### 返回值

* `result` (dict): 查询结果，包含 DAG 的维度信息。

* * *

```python

def get_dependencies_dag_ids_by_db(self, dag_id):
```

通过数据库查询获取指定 DAG 的依赖 DAG ID 列表。

### 参数

*   `dag_id` (str): 要查询依赖关系的 DAG 的 ID。

### 返回值

*   `dag_ids` (list): 依赖 DAG ID 的列表。

* * *

```python

def get_dag_state_by_db(self, dag_id):`
```
通过数据库查询获取指定 DAG 的最新运行状态。

### 参数

* `dag_id` (str): 要查询运行状态的 DAG 的 ID。

### 返回值

* `result` (dict): 查询结果，包含最新运行状态。

* * *

```python

def get_dag_state_by_api(self, dag_id):
```

通过 API 查询获取指定 DAG 的运行状态。

### 参数

* `dag_id` (str): 要查询运行状态的 DAG 的 ID。

### 返回值

* `dag_state` (str): 查询结果，表示 DAG 的运行状态。

***

## 示例用法

```python

# 创建 AirflowDagHandler 实例
dag_handler = AirflowDagHandler("http://43.143.250.12:8080")

# 通过数据库查询指定 DAG 的维度信息
dag_info = dag_handler.get_dag_info("dag_cedc_sales_pub")
print(dag_info)

# 通过数据库查询指定 DAG 的依赖 DAG ID 列表
dependencies_dag_ids = dag_handler.get_dependencies_dag_ids_by_db("dag_cedc_sales_pub")
print(dependencies_dag_ids)

# 通过数据库查询指定 DAG 的最新运行状态
dag_state_by_db = dag_handler.get_dag_state_by_db("dag_cedc_sales_landing")
print(dag_state_by_db)

# 通过 API 查询指定 DAG 的运行状态
dag_state_by_api = dag_handler.get_dag_state_by_api("dag_cedc_sales_landing")
print(dag_state_by_api)`

```

以上是 `AirflowDagHandler` 类的功能和使用示例。该类提供了通过数据库和 API 查询 DAG 相关信息的能力，可以用于获取 DAG 的维度信息、依赖 DAG ID 列表以及最新运行状态