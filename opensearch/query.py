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
from opensearch.exception import ArgumentError, QueryError

RELATION_OPERATOR = ('>', '<', '=', '<=', '>=', '!=')

ARITHMETIC_OPERATOR = ('+', '-', '*', '/', '&', '^', '|')


class RawQuery(object):

    def __init__(self):
        self.stmts = dict()

    def build(self):
        if not self.stmts.has_key('query'):
            raise QueryError('query statement required.')
        return self._fmt_dict(self.stmts)

    def _statement(self, stmt_type, stmt):
        self.stmts[stmt_type] = stmt
        return self

    def query(self, query_stmt):
        return self._statement('query', query_stmt)

    def config(self, config_stmt):
        return self._statement('config', config_stmt)

    def filter(self, filter_stmt):
        return self._statement('filter', filter_stmt)

    def sort(self, sort_stmt):
        return self._statement('sort', sort_stmt)

    def aggregate(self, aggregate_stmt):
        return self._statement('aggregate', aggregate_stmt)

    def distinct(self, distinct_stmt):
        return self._statement('distinct', distinct_stmt)

    def kvpair(self, kvpair_stmt):
        return self._statement('kvpair', kvpair_stmt)

    def _fmt_dict(self, dct, fmt="%s=%s", spliter="&&"):
        ss = []
        for key in dct:
            ss.append(fmt % (key, dct[key]))
        return spliter.join(ss)


class SimpleQuery(RawQuery):

    def __init__(self):
        super(SimpleQuery, self).__init__()
        self.sorts = []
        self.kvpairs = {}
        self.aggregates = []

    def build(self):
        if self.sorts:
            self.sort(";".join(self.sorts))
        if self.kvpairs:
            self.kvpair(self._fmt_dict(self.kvpairs, "%s:%s", ","))
        if self.aggregates:
            self.aggregate(";".join(self.aggregates))
        return super(SimpleQuery, self).build()

    def query_by_keyword(self, keyword):
        return self.query_by('default', keyword)

    def query_by(self, field, keyword, boost=None):
        if boost and isinstance(boost, int):
            stmt = "%s:%s^%d" % (field, keyword, boost)
        else:
            stmt = "%s:%s" % (field, keyword)
        return self.query(stmt)

    def config_by(self, start=0, hint=10, format_='json', rerank_size=200):
        stmt = "start:%s,hint:%s,format:%s,rerank_size:%s" % (start, hint, format_, rerank_size)
        return self.config(stmt)

    def filter_by(self, field, operator, value):
        if isinstance(value, basestring):
            if operator not in ARITHMETIC_OPERATOR:
                raise ArgumentError("only support arithmetic operator if value is string")
            stmt = "%s%s\"%s\"" % (field, operator, value)
        else:
            if operator not in ARITHMETIC_OPERATOR and operator not in RELATION_OPERATOR:
                raise ArgumentError("invalid operator: %s" % operator)
            stmt = "%s%s%s" % (field, operator, value)
        return self.filter(stmt)

    def add_sort(self, field, asc=True):
        self.sorts.append("%s%s" % (asc and '+' or '-', field))
        return self

    def set_kvpair(self, key, value):
        self.kvpairs[key] = value
        return self

    def add_aggregate(self, group_key, agg_funs, range_, agg_filter=None,
                      agg_sampler_threshold=None, agg_sampler_step=None, max_group=None):
        aggregate_args = {
            'group_key': group_key,
            'agg_fun': '#'.join(agg_funs),
            'range': range_
        }
        if agg_filter:
            aggregate_args['agg_filter'] = agg_filter
        if agg_sampler_threshold:
            aggregate_args['agg_sampler_threshold'] = agg_sampler_threshold
        if agg_sampler_step:
            aggregate_args['agg_sampler_step'] = agg_sampler_step
        if isinstance(max_group, int):
            aggregate_args['max_group'] = max_group

        self.aggregates.append(self._fmt_dict(aggregate_args, "%s:%s", ','))
        return self

    def distinct_by(self, dist_key, dist_times=None, dist_count=None, reserved=None,
                    update_total_hit=None, dist_filter=None, grade=None):
        distinct_args = dict(dist_key=dist_key)
        if isinstance(dist_times, int):
            distinct_args['dist_times'] = dist_times
        if isinstance(dist_count, int):
            distinct_args['dist_count'] = dist_count
        if isinstance(reserved, bool):
            distinct_args['reserved'] = reserved and 'true' or 'false'
        if isinstance(update_total_hit, bool):
            distinct_args['update_total_hit'] = update_total_hit and 'true' or 'false'
        if dist_filter:
            distinct_args['dist_filter'] = dist_filter
        if isinstance(grade, float):
            distinct_args['grade'] = grade

        stmt_list = []
        for key in distinct_args:
            stmt_list.append("%s:%s" % (key, distinct_args[key]))
        distinct_stmt = ",".join(stmt_list)
        return self.distinct(distinct_stmt)

    def distinct_by_unique(self, dist_key):
        self.set_kvpair('duniqfield', dist_key)
        return self.distinct_by(dist_key, reserved=False)
