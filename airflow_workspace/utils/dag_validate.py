import ast
import os
import sys

class DAGCheckError(Exception):
    pass

def check_operator_class(class_names, filepath):
    errors = []
    for class_name in class_names:
        if 'Operator' in class_name:
            if class_name != 'BashOperator':
                errors.append(f"Class name {class_name} in file {filepath} is not BashOperator")
    return errors

def get_instantiated_class_names(filename):
    class_names = []
    with open(filename, 'r') as file:
        tree = ast.parse(file.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(node.value, ast.Call):
                call = node.value
                if isinstance(call.func, ast.Name):
                    class_name = call.func.id
                    class_names.append(class_name)

    return class_names

def check_classes_in_directory(directory):
    errors = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                class_names = get_instantiated_class_names(filepath)
                class_errors = check_operator_class(class_names, filepath)
                if class_errors:
                    errors.extend(class_errors)

    if errors:
        error_messages = "\n".join(errors)
        raise DAGCheckError(f"DAG检查未通过:\n{error_messages}")

def main(directory):
    try:
        check_classes_in_directory(directory)
        print("DAG检查通过")
    except DAGCheckError as e:
        print(f"{str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("请提供要检查的DAG文件夹路径作为命令行参数")
        sys.exit(1)

    directory = sys.argv[1]
    main(directory)
