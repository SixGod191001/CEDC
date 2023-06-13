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
        node = node.replace('\n', '')  # 去掉多余的换行符
        node = node.replace(' ', '')  # 去掉多余的空格
        pattern = r'"([^"]+)":"([^"]+)"'  # 匹配属性键值对
        matches = re.findall(pattern, node)  # 查找所有的属性键值对
        _dict = dict(matches)  # 将属性键值对转换为字典
        return _dict.get(property_key)  # 获取指定属性键的值
    def split_string(string):
        result = []
        stack = []
        start = 0
        for i, char in enumerate(string):
            if char == '{':
                stack.append(char)
            elif char == '}':
                stack.pop()
            elif char == ',' and len(stack) == 0:
                result.append(string[start:i])
                start = i + 1
        result.append(string[start:])
        return result
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
