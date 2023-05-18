import ast
import os
import sys
from airflow_workspace.utils.postgre_handler import PostgresHandler

class DAGCheckError(Exception):
    pass

def check_operator_class(class_names, filepath):
    errors = []
    for class_name in class_names:
        if 'Operator' in class_name:
            if class_name != 'BashOperator':
                errors.append(f"Class name {class_name} in file {filepath} is not BashOperator")
    return errors

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

def main(directory, tags):
    try:
        check_classes_in_directory(directory, tags)
        print("DAG检查通过")
        return True
    except DAGCheckError as e:
        print(f"{str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("请提供要检查的DAG文件夹路径和允许的标签列表作为命令行参数")
        sys.exit(1)

    directory = sys.argv[1]
    # directory = r"C:\CEDC\airflow_workspace\dags"
    conn = PostgresHandler()
    sql = "select distinct tag from dim_dag"
    tags = conn.get_record(sql)

    tags = tags[0]['tag']
    print("允许的tags" + tags)
    main(directory, tags)
