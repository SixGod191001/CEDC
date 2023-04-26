from urllib.request import urlopen

import chardet


class FileTool:
    def __init__(self, urlORfilepath = None ):
        """
        :param urlORfilepath (str) : 需要读取的url链接或者文件路径
        """
        self.urlORfilepath = urlORfilepath
    def read_file(self):
        """
        读取文件，返回文件内容
        :return: text 返回文件内容
        """
        print("urlORfilepath" + self.urlORfilepath)
        if self.urlORfilepath.startswith('http'):
            self.text = self.read_url_file()
        else:
            self.text = self.read_server_file()
        return self.text
    def read_url_file(self):
        """
        读取url指向的文件，返回文件内容
        :return: text 返回文件内容
        """
        with urlopen(self.urlORfilepath) as f:
            text = f.read().decode('utf-8')
        return text

    def read_server_file(self):
        """
        读取文件，返回文件内容
        :return: text 返回文件内容
        """
        # 读取文件并检测编码格式
        with open(self.urlORfilepath, 'rb') as file:
            rawdata = file.read()
            result = chardet.detect(rawdata)
            encoding = result['encoding']
        with open(self.urlORfilepath, 'r', encoding=encoding) as file:
            text = file.read()
        return text
    def write_file(self,content):
        """
        向目标文件写入内容
        """
        with open(self.urlORfilepath, 'w') as file:
            file.write(content)

    # def upload_s3_file(self,s3_bucket_name,s3_file_path):
    #     s3 = boto3.client('s3')
    #     with open(self.filepath, "rb") as f:
    #         s3.upload_fileobj(f, s3_bucket_name, s3_file_path)


# if __name__ == '__main__':
#     u_unix = 'https://bkt-dfk.s3.ap-northeast-1.amazonaws.com/workspace/forremote/tmp/test_unix.sql?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjELH%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLXNvdXRoZWFzdC0xIkgwRgIhAOxjVqurmSI2mA1BT26m4KMNyMqYSgOVkkWWaI%2FoPvl6AiEA4zZKuIPX8zaaaO%2F0C4sBMcF%2FJgsfBvMi4LRpOHzrfHcqjgMImv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAAGgwwNjQwNTU1NDUwMTgiDDf4W0ol38dpNF4MESriAsAD4quC%2FejQLSqqZufEINhFm5FKMRv531ldp3TYZYuhMQOHsscrOi%2FkH2uJerDdjZX44ifMcHcEMs9tAPapiF8o6idt8qkwbcxEKIOEvtcPx8N%2FTjQfgOpHHfHH%2FlAUdNQ9Jrg8ONM4%2F%2FX%2BrSQQAMhSHdjcqopLbYIKWIRRuvjsskWuJh%2FE7Ud8jn4qDwM3%2BfclN2I5HrdKvRPXuwNE7Y1LZze5EcoTFzHm7LYauvKGTQKR8xS3snnWmYAk95c%2F2gSeqRJVmCLJSv1JbsXHRdiqpEyuEeqTJrhleOcBvaBoLWQmvMN6is65CUxkNFHMhEk2nfBgUVNbAKqw6jD1ypuDy7y4x755A%2F76aLnaP0q8Wrgzs1v9No7WtMbDFF7dtjP0T%2Fu0aHpzUwwrwBcpQCBDTtcg9ZhvEdin6VXskIEZGM3c2DXJ0KJ7I%2BHRFhZqHy0ePE86sqEL0sxjI%2BM%2F4wuCtDDT3dKhBjqyAhKoIAgPLvgIH6Oex1oTeWE5bbd1w%2FCRHHNeoC56Miv5mHssfQdwgateEa%2BWYZaeYy6m4Cj5liwhN4bgHhu%2FfUTd3XR36xMqzM7zMuDxj%2BGBx8EU5YpkwlJWvmFMQOiLuG2Juwhj88w1LF8v6aYf2GZhsZpIGrC0A1DWOf8KI02VUfhe8%2B7VE3KNlD0aAkhCBzuXfi%2Bx5aV5O7Icpi0QxlwIhROJ5G5ddmckYNSbfW8jjpka3uLlWyc0FleegRA7eYT7TGWCr42G9XOl1v5%2FgcToxg09HhtM8pWBXUDknQqefw8BkoBGoWG0eeGISrYyA8cn99ZyYn%2Bai%2BoyEnHOuZ0au%2Bnh0puBGEW4Rvobb1%2BZ7XYVsDpAe3XeqOCfmwI6ERUXuPOcMOOHDGK1oANSYnfNew%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230411T013404Z&X-Amz-SignedHeaders=host&X-Amz-Expires=32399&X-Amz-Credential=ASIAQ52QCBS5NXWIFPJW%2F20230411%2Fap-northeast-1%2Fs3%2Faws4_request&X-Amz-Signature=ae352034193ea402d5d45ef55549214a6211c4ee9a227c4378bd438892154dfc'
#     ft= FileTool(u_unix)
#     print(ft.read_url_file())
#     local_file = 'D:\workspace\python\CEDC\workspace\glue\glue-script-generator\py_head_part.txt'
#     ft= FileTool(local_file)
#     print(ft.read_file())
#
#     # s3='bkt-dfk'
#     # s3_path='/workspace/forremote/tmp/'
#     # local_file='D:\workspace\python\CEDC\workspace\glue\glue-script-generator\py_head_part.txt'
#     # ft.upload_s3_file(s3,s3_path)
