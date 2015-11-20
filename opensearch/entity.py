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
import json


class Entity(object):

    def __str__(self):
        attr_names = self.__dict__
        attrs = ["%s=%s" % (attr_name, getattr(self, attr_name)) for attr_name in attr_names]
        return ",".join(attrs)

    def to_dict(self, excludes=(), include_none_value=False, **kwargs):
        dicts = dict()
        attr_names = self.__dict__
        for attr in attr_names:
            if not attr.startswith('_') and attr not in excludes:
                v = getattr(self, attr)
                if not include_none_value and v is None:
                    continue
                if isinstance(v, Entity):
                    v = v.to_dict()
                if attr in kwargs:
                    dicts[kwargs.get(attr)] = v
                else:
                    dicts[attr] = v
        return dicts

    def from_dict(self, dicts, append=False):
        for key in dicts:
            if key in self.__dict__ or append:
                setattr(self, key, dicts.get(key))
        return self


class SearchSummary(Entity):

    def __init__(self, field, element=None, ellipsis=None,
                 snipped=None, length=None, prefix=None, postfix=None):
        self.summary_field = field
        self.summary_element = element
        self.summary_ellipsis = ellipsis
        self.summary_snipped = snipped
        self.summary_len = length
        self.summary_prefix = prefix
        self.summary_postfix = postfix

    def to_string(self):
        attrbutes = self.__dict__
        strings = []
        for name in attrbutes:
            value = getattr(self, name)
            if value is not None:
                strings.append("%s:%s" % (name, value))
        return ",".join(strings)


class SearchResult(Entity):

    def __init__(self):
        self.searchtime = None
        self.total = None
        self.num = None
        self.viewtotal = None
        self.facet = None


class DocumentField(Entity):

    def __init__(self):
        self.id = None
