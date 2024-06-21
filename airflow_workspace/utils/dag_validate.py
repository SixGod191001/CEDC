import ast
import os
import sys
from airflow_workspace.utils.postgre_handler import PostgresHandler

# 脚本用于检查Airflow中的DAG文件，包括检查操作符类名和tags是否符合规定。如果检查未通过，会抛出自定义异常并输出错误信息。如果检查通过，会打印提示信息并返回True。

# 定义一个自定义异常类DAGCheckError，用于在检查过程中抛出错误。
class DAGCheckError(Exception):
    pass

# check_operator_class函数用于检查DAG文件中的操作符类名是否符合规定。它接受两个参数：class_names（操作符类名列表）和filepath（DAG文件路径）。
# 在函数中，如果类名中包含"Operator"字符串且不是"BashOperator"，则将错误信息添加到errors列表中。最后，返回错误列表。
def check_operator_class(class_names, filepath):
    errors = []
    for class_name in class_names:
        if 'Operator' in class_name:
            if class_name != 'BashOperator':
                errors.append(f"Class name {class_name} in file {filepath} is not BashOperator")
    return errors

# check_tags函数用于检查DAG文件中的tags是否符合规定。它接受两个参数：tags（允许的标签列表）和filepath（DAG文件路径）。
# 函数中通过使用ast模块解析DAG文件的抽象语法树，并遍历树中的节点，查找包含params函数调用的节点。
# 然后，检查该函数调用的参数是否包含一个名为"tags"的键，以及该键对应的值是否为一个列表。如果发现不符合规定的情况，会抛出自定义异常DAGCheckError。
# 如果所有条件都符合，函数返回True。
def check_tags(tags, filepath):
    with open(filepath, 'r' , encoding='utf-8') as file:
        tree = ast.parse(file.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'params':
            if isinstance(node.args[0], ast.Dict):
                for key in node.args[0].keys:
                    if isinstance(key, ast.Str) and key.s == 'tags':
                        if isinstance(node.args[0].values[0], ast.List):
                            for tag in node.args[0].values[0].elts:
                                if isinstance(tag, ast.Str) and tag.s not in tags:
                                    raise DAGCheckError(f"Tag '{tag.s}' in file {filepath} is not allowed.")
                        else:
                            raise DAGCheckError(f"Tag value in file {filepath} must be a list.")
                        return True
                raise DAGCheckError(f"Tag key not found in file {filepath}.")
            else:
                raise DAGCheckError(f"Tag value in file {filepath} must be a dictionary.")

    raise DAGCheckError(f"Tag not found in file {filepath}.")

# get_instantiated_class_names函数用于获取DAG文件中实例化的类名列表。它接受一个参数filename（DAG文件路径）。
# 函数中同样使用ast模块解析DAG文件的抽象语法树，并遍历树中的节点，查找赋值语句中的类实例化节点。将实例化的类名添加到class_names列表中，最后返回该列表。
def get_instantiated_class_names(filename):
    class_names = []
    with open(filename, 'r', encoding='utf-8') as file:
        tree = ast.parse(file.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(node.value, ast.Call):
                call = node.value
                if isinstance(call.func, ast.Name):
                    class_name = call.func.id
                    class_names.append(class_name)

    return class_names

# check_classes_in_directory函数用于检查指定目录下的所有DAG文件中的类和tags是否符合规定。
# 它接受两个参数：directory（要检查的DAG文件夹路径）和tags（允许的标签列表）。
# 函数通过遍历指定目录下的所有.py文件，调用get_instantiated_class_names函数获取类名列表，并调用check_operator_class函数检查操作符类名，
# 最后调用check_tags函数检查tags。如果发现任何不符合规定的情况，将错误信息添加到errors列表中。如果存在错误，会抛出自定义异常DAGCheckError，否则返回True。
def check_classes_in_directory(directory, tags):
    errors = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                class_names = get_instantiated_class_names(filepath)
                class_errors = check_operator_class(class_names, filepath)
                if class_errors:
                    errors.extend(class_errors)
                try:
                    check_tags(tags, filepath)
                except DAGCheckError as e:
                    errors.append(str(e))

    if errors:
        error_messages = "\n".join(errors)
        raise DAGCheckError(f"DAG检查未通过:\n{error_messages}")

    return True

# main函数用于执行检查操作。它接受两个参数：directory（要检查的DAG文件夹路径）和tags（允许的标签列表）。
# 在函数中，调用check_classes_in_directory函数进行DAG文件的检查。如果检查通过，打印"DAG检查通过"并返回True，否则打印错误信息并返回False。
def main(directory, tags):
    try:
        check_classes_in_directory(directory, tags)
        print("DAG检查通过")
        return True
    except DAGCheckError as e:
        print(f"{str(e)}")
        return False

# 使用sys.argv获取命令行参数，获取要检查的DAG文件夹路径，并通过PostgresHandler类从数据库中获取允许的标签列表。然后调用main函数进行检查
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("请提供要检查的DAG文件夹路径和允许的标签列表作为命令行参数")
        sys.exit(1)

    directory = sys.argv[1]
    # directory = r"C:\CEDC\airflow_workspace\dags"
    conn = PostgresHandler()
    sql = "select distinct tag from dim_dag"
    tags = conn.execute_select(sql)

    # 将查询结果列表中的第一个元素的'tag'键对应的值赋给tags变量。
    tags = tags[0]['tag']
    print("允许的tags" + tags)
    main(directory, tags)
