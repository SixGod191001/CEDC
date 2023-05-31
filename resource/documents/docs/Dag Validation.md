# DAG检查工具文档

## 1. 概述

该工具用于检查Airflow中的DAG文件，主要包括两方面的检查：操作符类名和tags的规范性。如果检查未通过，工具会抛出自定义异常并输出错误信息；如果检查通过，工具会打印提示信息并返回True。

## 2. 功能需求

### 2.1 自定义异常类DAGCheckError

+ 该工具定义了一个自定义异常类`DAGCheckError`，用于在检查过程中抛出错误。

### 2.2 检查操作符类名

+ `check_operator_class`函数用于检查DAG文件中的操作符类名是否符合规定。
+ 输入参数：
  + `class_names`：操作符类名列表
  + `filepath`：DAG文件路径
+ 函数逻辑：
  + 遍历操作符类名列表，如果类名中包含"Operator"字符串且不是"BashOperator"，则将错误信息添加到错误列表`errors`中。
  + 返回错误列表。

### 2.3 检查tags

+ `check_tags`函数用于检查DAG文件中的tags是否符合规定。
+ 输入参数：
  + `tags`：允许的标签列表
  + `filepath`：DAG文件路径
+ 函数逻辑：
  + 使用`ast`模块解析DAG文件的抽象语法树，并遍历树中的节点，查找包含`params`函数调用的节点。
    + 检查`params`函数调用的参数是否包含一个名为"tags"的键，以及该键对应的值是否为一个列表。
    + 如果发现不符合规定的情况，会抛出自定义异常`DAGCheckError`。
    + 如果所有条件都符合，函数返回True。

### 2.4 获取实例化的类名

+ `get_instantiated_class_names`函数用于获取DAG文件中实例化的类名列表。
+ 输入参数：
  + `filename`：DAG文件路径
+ 函数逻辑：
  + 使用`ast`模块解析DAG文件的抽象语法树，并遍历树中的节点，查找赋值语句中的类实例化节点。
  + 将实例化的类名添加到类名列表`class_names`中。
  + 返回类名列表。

### 2.5 检查指定目录下的所有DAG文件

+ `check_classes_in_directory`函数用于检查指定目录下的所有DAG文件中的类和tags是否符合规定。
+ 输入参数：
  + `directory`：要检查的DAG文件夹路径
  + `tags`：允许的标签列表
+ 函数逻辑：
  + 遍历指定目录下的所有.py文件。
  + 调用`get_instantiated_class_names`函数获取类名列表，并调用`check_operator_class`函数检查操作符类名，将错误信息添加到错误列表`errors`中。
  + 调用`check_tags`函数检查tags，如果发现任何不符合规定的情况，将错误信息添加到错误列表`errors`中。
  + 如果存在错误，抛出自定义异常`DAGCheckError`，否则返回True。

### 2.6 执行检查操作

+ `main`函数用于执行检查操作。
+ 输入参数：
  + `directory`：要检查的DAG文件夹路径
  + `tags`：允许的标签列表
+ 函数逻辑：
  + 调用`check_classes_in_directory`函数进行DAG文件的检查。
  + 如果检查通过，打印"DAG检查通过"并返回True，否则打印错误信息并返回False。

### 2.7 命令行参数解析与执行

+ 使用`sys.argv`获取命令行参数，获取要检查的DAG文件夹路径，并通过`PostgresHandler`类从数据库中获取允许的标签列表。
+ 调用`main`函数进行检查。
+ 如果命令行参数个数不为2，打印错误提示信息并退出。
+ 获取要检查的DAG文件夹路径和允许的标签列表。
+ 调用`main`函数进行检查。

## 3. 输入/输出

### 3.1 输入

+ 命令行参数：
  + DAG文件夹路径
+ 数据库中允许的标签列表

### 3.2 输出

+ 根据检查结果，输出以下内容之一：
  + 如果检查通过，打印"DAG检查通过"并返回True。
  + 如果检查未通过，打印错误信息并返回False。

## 4. 技术栈

+ Python
+ `ast`模块
+ Airflow

## 5. 依赖

+ `airflow_workspace.utils.postgre_handler`模块
+ `PostgresHandler`类
+ Airflow

## 6. 使用示例

```python
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("请提供要检查的DAG文件夹路径和允许的标签列表作为命令行参数")
        sys.exit(1)

    directory = sys.argv[1]
    # directory = r"C:\CEDC\airflow_workspace\dags"
    conn = PostgresHandler()
    sql = "select distinct tag from dim_dag"
    tags = conn.get_record(sql)

    # 将查询结果列表中的第一个元素的'tag'键对应的值赋给tags变量。
    tags = tags[0]['tag']
    print("允许的tags" + tags)
    main(directory, tags)
```

***
以上是对DAG检查工具的需求文档描述，该工具主要用于检查Airflow中的DAG文件的操作符类名和tags是否符合规定。根据命令行参数提供的DAG文件夹路径和数据库中的允许标签列表进行检查，并输出检查结果。