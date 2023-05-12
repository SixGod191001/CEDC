import os
import ast

def find_operator_classes(directory):
    operator_class_names = []

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    try:
                        tree = ast.parse(f.read())
                    except SyntaxError:
                        # Ignore files with syntax errors
                        continue

                for node in ast.walk(tree):
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and hasattr(node.func, "id"):
                        class_name = node.func.id
                        if "Operator" in class_name:
                            operator_class_names.append((class_name, file_path))

    return operator_class_names

# 指定要搜索的目录路径
directory_path = r'D:\CEDC\airflow_workspace\dags'

# 查找包含 "Operator" 的类名及所在文件
result = find_operator_classes(directory_path)

# 检查类名并报错
for class_name, file_path in result:
    if class_name != "BashOperator":
        raise ValueError(f"类名 {class_name} 不是 BashOperator，位于文件 {file_path}")

# 打印结果
print("所有类名均为 BashOperator")
