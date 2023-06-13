import random
import re


class NodeTool:
    def __init__(self):
        pass

    def get_ctx_name(self, node_name):
        """
        :param node_name: ctx_name 的前缀
        :return: ctx_name 例：SQLTransform_node1119144207939
        """
        ctx_node_name = "{node_name}_node{random_id}".format(node_name=node_name,
                                                             random_id=random.randint(1000000000001,
                                                                                      1999999999999))
        return ctx_node_name

    def node_to_dict(self, nodes):
        """
        :param nodes: 包含node的set
        :return: 以nodename为key的字典，value为node小括号中的内容转换成的字典
        """
        result_dict = {}
        for node in nodes:
            node = node.replace('\n', '')  # 去掉多余的换行符
            node = node.replace(' ', '')  # 去掉多余的空格
            nodename = node.split('=')[0]  # 获得node的名字
            pattern = r'[^(]*\((.*?),?\)[^)]*'  # 获得最外层括号中的内容
            result = re.search(pattern, node).group(1)  # 获取匹配结果
            _dict = {}  # 将外层括号中的内容封装成字典
            for item in result.split(','):
                key, value = item.split('=')
                _dict[key] = value.strip('"')
            result_dict.update({nodename: _dict})
        return result_dict

    def get_node_name(self, node):
        """
        :param node:
        :return: nodename
        """
        # 使用正则表达式获取第一个包含=行的=前的字符
        match = re.search(r'^.*?(\w+)\s*=\s*', node, re.MULTILINE)

        # 如果匹配成功，则获取第一个匹配的组
        if match:
            nodename = match.group(1)
        else:
            nodename = None
        return nodename

    def get_node_property_value(self, node, property_key):
        """
        :param node:
        :param property_key: 属性的key
        :return: 属性的value
        """
        node = node.replace('\n', '')  # 去掉多余的换行符
        node = node.replace(' ', '')  # 去掉多余的空格
        print(node)
        print("11===========================================================================================")
        pattern = r'[^(]*\((.*?),?\)[^)]*'  # 获得最外层括号中的内容
        result = re.search(pattern, node).group(1) # 获取匹配结果
        pattern = r'\{[^{}]+\}'
        result = re.sub(pattern, lambda m: m.group().replace(',', ';'), result)
        _dict = {}  # 将外层括号中的内容封装成字典
        print(result)
        print("22===========================================================================================")
        #for item in result.split(','):
        for item in re.split(r",(?![^{]*\})", result):
            print("===========================================================================================")
            print(item)
            key, value = item.split('=')
            print("-------------------------------------------------------------------------------------------")
            _dict[key] = value.strip('"')
        return _dict[property_key]

# if __name__ == '__main__':
#     nt=NodeTool()
#     print(nt.get_ctx_name('xxx'))
#     a = '''sales_node1678341525471 = glueContext.create_dynamic_frame.from_catalog(
#     database="devops",
#     table_name="sales_csv",
#     transformation_ctx="sales_node1678341525471",
#
# )'''
#     print(a)
#     print((a,))
#     print(nt.node_to_dict((a,)))
#
#     print(nt.get_node_property_value(a,'table_name'))
