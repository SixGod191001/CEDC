class StrTool:
    def __init__(self):
        pass

    def add_enter_char(self, original_str):
        """
        给字符串最后追加一个回车符
        :param original_str: 需要编辑的字符串
        :return: 追加回车符后的字符串
        """
        return original_str + '\n'

    def concate_strings_with_enter_char(self, *args):
        """
        把所有参数中的字符串用\n拼接
        :param args: 需要拼接的子字符串
        :return: 拼接后的字符串
        """
        result_str = ''
        for arg in args:
            result_str += self.add_enter_char(arg)
        return result_str
