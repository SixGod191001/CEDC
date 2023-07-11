# Dependency类 - 需求文档

## 概述

`Dependency`类是验证airflow中依赖DAG的执行状态。它确保具有依赖的DAG在主DAG继续执行之前已成功完成。该类与Airflow组件（如Airflow Rest API和Metadata DB）交互，比较并Check依赖DAG的执行状态。

## 类构造函数

`Dependency`类的构造函数不接受任何参数。但是，它初始化了以下属性：

* `dag_ids`：使用`AirflowDagHandler`获取依赖DAG的DAGs列表
* `base_url`：Airflow Web服务器URL的字符串变量。

## 方法

### `check_dependencies(event)`

该方法执行主DAG的依赖检查。它从`event`参数中检索所需的信息，并与Airflow Rest API、Metadata DB交互，验证依赖DAG的执行状态。

#### 输入

* `event`（字典）：包含依赖检查所需信息的字典。
  * `dag_id`（字符串）：主DAG的ID。
  * `base_url`（字符串，可选）：Airflow Web服务器URL。

#### 输出

* Check的结果

#### 行为

* 从`event`参数中获取`dag_id`和`base_url`。
* 使用`base_url`和`dag_id`从`AirflowDagHandler`类获取等待时间和最大等待次数。
* 使用`AirflowDagHandler`获取依赖DAG的DAGs列表，并将其存储在`dag_ids`属性中。
* 如果没有依赖DAG（`dag_ids`为空），记录一条消息并退出方法。
* 对于每个依赖的DAG ID（`dag_id`）
  * 使用`AirflowDagHandler`和`dag_id`从Airflow Rest API和Metadata DB获取依赖DAG的执行状态；
  * 比较从API和DB获取的执行状态；
  * 如果状态不匹配，记录警告信息并发送电子邮件通知；
  * 如果DB中DAG的执行状态为"success"，记录成功消息并中断循环；
  * 如果DB中DAG的执行状态为"failed"，记录失败消息并引发AirflowFailException。
