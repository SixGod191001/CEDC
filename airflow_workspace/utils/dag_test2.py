import os
import glob
import ast

# 指定文件夹路径
folder_path = r'D:\CEDC\airflow_workspace\dags'

# 获取文件夹下所有的Python文件
python_files = glob.glob(os.path.join(folder_path, '*.py'))

# 存储实例化的类名和文件信息
class_info = []

# 遍历每个Python文件
for file_path in python_files:
    with open(file_path, 'r') as file:
        # 解析AST
        tree = ast.parse(file.read())

        # 遍历AST节点
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and hasattr(node.func, "id"):
                class_name = node.func.id
                if "BashOperator" in class_name:
                    pass
                else:
                    class_info.append(("ERROR", class_name, file_path))

# 打印实例化的类信息
print("Class Information:")
for info in class_info:
    if info[0] == "ERROR":
        print(f"ERROR: Class '{info[1]}' in file '{info[2]}'")
    else:
        print(f"True: Class '{info[0]}' in file '{info[1]}'")
