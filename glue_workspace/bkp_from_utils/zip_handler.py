import zipfile
import os
import sys

def zip_files(files_path, zip_filename):
    # 指定保存路径
    zip_path = os.path.join(files_path, zip_filename)
    
    # 判断压缩文件是否已存在，如果存在则删除
    if os.path.exists(zip_path):
        os.remove(zip_path)
    
    # 创建压缩文件
    zip_file = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED)
    
    # 遍历当前路径下的所有文件和文件夹
    for root, dirs, files in os.walk(files_path):
        for file in files:
            if file.endswith(".py"):
                # 构造文件的绝对路径
                file_path = os.path.join(root, file)
                
                # 将文件添加到压缩文件中，并指定替换已存在的文件
                zip_file.write(file_path, os.path.relpath(file_path, files_path))
    
    # 关闭压缩文件
    zip_file.close()

# 从命令行获取文件路径和压缩文件名
files_path = sys.argv[1]
zip_filename = sys.argv[2]

# 调用函数进行压缩
zip_files(files_path, zip_filename)
