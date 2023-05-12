# import os
# import ast
#
# def find_instantiated_classes(directory):
#     class_names = []
#
#     for root, _, files in os.walk(directory):
#         for file in files:
#             if file.endswith(".py"):
#                 file_path = os.path.join(root, file)
#                 print(file_path)
#                 with open(file_path, "r") as f:
#                     try:
#                         tree = ast.parse(f.read())
#                         print(tree)
#                     except SyntaxError:
#                         # Ignore files with syntax errors
#                         continue
#
#                 for node in ast.walk(tree):
#                     # 代码通过遍历 ast 抽象语法树中的所有节点（包括类定义、函数定义、函数调用等等），来找到所有的类定义节点（ast.ClassDef 类型），
#                     # 并将它们的名称（node.name）添加到 class_names 列表中。
#                     if isinstance(node, ast.ClassDef):
#                         class_names.append(node.name)
#                     # 代码的目的是从 ast 抽象语法树中提取实例化的类名。因为在Python中，类的实例化通常是通过调用类的构造函数来完成的，
#                     # 所以这段代码可以帮助我们找到代码中实例化的类名。
#                     if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and hasattr(node.func, "id"):
#                         print(node)
#                         class_names.append(node.func.id)
#
#     return class_names
#
# # 指定要搜索的目录路径
# directory_path = r"D:\CEDC\airflow_workspace\dags"
#
# # 查找实例化的类名
# result = find_instantiated_classes(directory_path)
#
# # 打印结果
# print("实例化的类名：")
# for class_name in result:
#     print(class_name)


import os
import ast

def extract_class_instantiations(file_path):
    class_instantiations = []

    with open(file_path, 'r') as file:
        tree = ast.parse(file.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and isinstance(node.value, ast.Call):
                        class_name = node.value.func.id
                        class_instantiations.append(class_name)

    return class_instantiations

def check_bash_operator(class_name):
    if 'Operator' in class_name and 'BashOperator' in class_name:
        return True
    return False

def find_bash_operators(directory_path):
    bash_operators = []

    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                class_instantiations = extract_class_instantiations(file_path)

                for class_name in class_instantiations:
                    print("Class instantiated:", class_name)
                    if check_bash_operator(class_name):
                        print("DAG validation passed.")
                        bash_operators.append(file_path)
                        break
                    else:
                        print("DAG validation failed.")

    return bash_operators

directory_path = r'D:\CEDC\airflow_workspace\dags'
bash_operators = find_bash_operators(directory_path)

if len(bash_operators) > 0:
    print("BashOperator found in the following files:")
    for file_path in bash_operators:
        print(file_path)
else:
    print("No BashOperator found in the specified directory.")
