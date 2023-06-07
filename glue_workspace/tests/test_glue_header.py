# -*- coding: utf-8 -*-
import unittest
import re
import os
from glue_workspace.script.glue_script_generator.header_dynamic import Headertransform

def remove_number(string):
    return re.sub(r'[0-9]+', '', string)


class TestHeader(unittest.TestCase):
    def test_header_file(self):
        m = Headertransform(type='s3')
        s, t = m.write_header_frame()
        self.assertIsNotNone(s, t)




