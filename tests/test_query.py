#coding:utf8

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

from opensearch import log
from opensearch.query import RawQuery, SimpleQuery


class RawQueryTest(unittest.TestCase):

    def test__statement(self):
        q = RawQuery()
        q.query("title:'北京大学'")
        self.assertEqual(q.build(), "query=title:'北京大学'")


class SimpleQueryTest(unittest.TestCase):

    def test_query(self):
        q = SimpleQuery()
        stmt = q.query_by_keyword('Kobe').\
            add_sort('name').\
            add_sort('age', False).\
            distinct_by_unique('score').\
            filter_by('steal', '>', 2).\
            build()
        log.debug(stmt)
