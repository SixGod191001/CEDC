## Dependency Check 模块文档

该模块用于检查 DAG 的依赖关系的运行状态。

### 模块说明

- **作者：** Liu Zhu
- **日期：** 2023/5/26 13:31

### 类 `Dependency`

#### 方法`check_dependencies(event)`

该方法用于检查 DAG 的依赖关系的运行状态。

**参数：**

- `event`（字典）: 包含以下键的字典:
  - `'dag_id'`（字符串）: DAG 的 ID。
  - `'base_url'`（字符串）: Airflow Web Server 的 URL。

**返回值：**

- 无

#### 属性

- `dag_ids`（字符串）: DAG 的 ID。

### 使用示例

```python
checker = Dependency()

event = {
    "dag_id": "dag_cedc_sales_pub",
    "base_url": "http://43.143.250.12:8080"
}

checker.check_dependencies(event)
```

在上述示例中，我们创建了一个 `Dependency` 的实例 `checker`，并设置了 `event` 参数，其中包含了 `dag_id` 和 `base_url`。然后，我们调用 `check_dependencies` 方法来检查 DAG 的依赖关系的运行状态。

请注意，你需要根据实际情况修改 `event` 参数中的值，以适应你的环境和需求。
