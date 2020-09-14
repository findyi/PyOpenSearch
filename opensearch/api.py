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
import json
import time

from opensearch.entity import SearchSummary
from opensearch.exception import ApiError, ArgumentError
from opensearch.httpclient import HttpClient
from opensearch.signature import Signature


class ApiResponse(object):

    def __init__(self, json_body):
        if not isinstance(json_body, dict):
            raise TypeError("json_body must be dict")

        self.status = json_body.get('status')
        self.errors = json_body.get('errors', {})

        self.request_id = json_body.get('request_id')
        if self.request_id is None:
            self.request_id = json_body.get('RequestId')
        self.data = json_body.get('result')

    def _get_request_id(self):
        return self

    def is_success(self):
        return self.status == 'OK'

    def error_code(self):
        return self.errors.get('code')

    def error_message(self):
        return self.errors.get('message')

    def __str__(self):
        return "status: %s errors: %s data: %s" % (self.status, self.errors, self.data)


class OpenSearchClient(object):

    def __init__(self, api_host, access_key_id, access_key_secret):
        self.api_host = api_host
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret

    def request(self, path, method='POST', **params):
        req_params = self._generate_common_params()
        req_params.update(params)
        signature = Signature().sign(self.access_key_secret, method, req_params)
        req_params['Signature'] = signature

        httpclient = HttpClient.get_httpclient()
        text_body = httpclient.request(self.api_host + path, method, req_params)
        resp = ApiResponse(json.loads(text_body))
        if not resp.is_success():
            raise ApiError(resp.error_code(), resp.error_message())
        else:
            return resp.data

    def _generate_common_params(self):
        nowtime = time.time()
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(nowtime)))
        nonce = str(int(nowtime * 1000000))

        comm_params = {
            'Version': 'v2',
            'AccessKeyId': self.access_key_id,
            'Signature': '',
            'SignatureMethod': 'HMAC-SHA1',
            'Timestamp': timestamp,
            'SignatureVersion': '1.0',
            'SignatureNonce': nonce
        }
        return comm_params


class Api(object):

    PATH_PREFIX = None

    def __init__(self, client, app_name):
        self.client = client
        self.app_name = app_name

    @property
    def path(self):
        return self.PATH_PREFIX + '/' + self.app_name

    def jsonify(self, obj):
        return json.dumps(obj, ensure_ascii=False)


class Application(Api):

    PATH_PREFIX = '/index'

    def create(self, template):
        """Create app by template

        :param template: tht app template name
        :return: app_name -> string
        """
        return self.client.request(self.path, action='create', template=template)

    def delete(self):
        """Delete app by name

        :return: app_name -> string
        """
        return self.client.request(self.path, action='delete')

    def info(self):
        """Get app information

        :return: app info -> dict
        """
        return self.client.request(self.path, action='status')

    def list(self, page, page_size):
        """Get all app list

        :param page:
        :param page_size:
        :return:
        """
        return self.client.request(self.PATH_PREFIX, page=page, page_size=page_size)

    def reindex(self):
        return self.client.request(self.path, method='GET', action='createtask')

    def reindex_import(self, table_names):
        table_name = None
        if isinstance(table_names, (list, tuple)):
            table_name = ",".join(table_names)
        return self.client.request(self.path, method='GET',
                                   action='createtask', operate='import', table_name=table_name)


class Document(Api):

    PATH_PREFIX = '/index/doc'

    def __init__(self, client, app_name, table_name):
        super(Document, self).__init__(client, app_name)
        self.table_name = table_name
        self._items = []

    def add(self, fields, timestamp=None):
        """Add Doucment fields

        :param fields: the document fields you push to server.
        :param timestamp: the document update time.
        """
        self._op('add', fields, timestamp)

    def update(self, fields, timestamp=None):
        """Update Doucment fields

        :param fields: the document fields you push to server.
        :param timestamp: the document update time.
        """
        self._op('update', fields, timestamp)

    def delete(self, fields, timestamp=None):
        """Delete Doucment fields

        :param fields: the document fields you push to server. you can only special the 'id' field.
        :param timestamp: the document update time.
        """
        self._op('delete', fields, timestamp)

    def _op(self, cmd, fields, timestamp=None):
        if fields is None or not isinstance(fields, dict):
            raise ArgumentError("fields is required and it must be dict type")
        if fields.has_key('id') or fields['id'] is None:
            raise ArgumentError("fields must contain 'id' key and it must be not None")

        item = dict(cmd=cmd, timestamp=timestamp, fields=fields)
        self._items.append(item)

    def push(self):
        """Push document operation to server.

        Before call this function. you should call add() or delete() or update() more than one times
        """
        if len(self._items) == 0:
            raise ArgumentError("please call add() or update() or delete() first.")
        return self.client.request(self.path, action='push', table_name=self.table_name,
                                   items=self.jsonify(self._items))


class Search(Api):

    PATH_PREFIX = '/search'

    @property
    def path(self):
        return self.PATH_PREFIX

    def search(self, query, index_names=None, fetch_fields=None, qp=None, disable=None,
               first_formula_name=None, formula_name=None, summary=None):
        """Search from server

        :param query: the query string.
        :param index_names:
        :param fetch_fields:
        :param qp:
        :param disable:
        :param first_formula_name:
        :param formula_name:
        :param summary:
        :return:
        """

        if isinstance(index_names, (list, tuple)) and len(index_names) > 0:
            index_name = ";".join(index_names)
        else:
            index_name = self.app_name

        if fetch_fields and not isinstance(fetch_fields, dict):
            raise ArgumentError("parameter 'fetch_fields' need dict type")
        if disable and disable != 'qp':
            raise ArgumentError("parameter 'disable' only supoort 'qp' now.")
        if summary and not isinstance(summary, SearchSummary):
            raise ArgumentError("parameter 'fetch_fields' need 'opensearch.entity.SearchSummary' type")

        kwargs = dict(
            query=query.build(),
            index_name=index_name,
            fetch_fields=fetch_fields,
            qp=qp,
            disable=disable,
            first_formula_name=first_formula_name,
            formula_name=formula_name,
            summary=summary.to_string()
        )
        return self.client.request(self.path, method='GET', **kwargs)


class Suggestion(Api):

    PATH_PREFIX = '/suggest'

    @property
    def path(self):
        return self.PATH_PREFIX

    def suggest(self, query_text, suggest_name, hint=None):
        kwargs = {
            'query': query_text,
            'index_name': self.app_name,
            'suggest_name': suggest_name
        }
        if isinstance(hint, int):
            kwargs['hint'] = hint
        return self.client.request(self.path, method='GET', **kwargs)


class ApiLog(Api):

    PATH_PREFIX = '/index/error'

    def get_log(self, page, page_size, sort_mode):
        kwargs = {
            'page': page,
            'page_size': page_size,
            'sort_mode': sort_mode
        }
        return self.client.request(self.path, **kwargs)
