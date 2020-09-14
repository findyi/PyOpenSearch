# coding:utf8

"""
Copyright 2015 tufei

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import unittest

from opensearch.signature import Signature


class SignatureTest(unittest.TestCase):

    def test_canonicalize_query_string(self):
        signature = Signature()
        querys = {
            'name': 'kobe',
            'age': 36,
            'team': 'Lakers'
        }
        canonicalized = signature.canonicalize_query_string(querys)
        self.assertEqual(canonicalized, "age=36&name=kobe&team=Lakers")

