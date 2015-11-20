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
import httplib
import urllib
import requests

from urlparse import urlparse
from opensearch import log
from opensearch.exception import HTTPError, ArgumentError


class HttpClient(object):

    def request(self, url, method, params):
        raise NotImplementedError

    @classmethod
    def get_httpclient(cls):
        return DefaultHttpClient()


class DefaultHttpClient(HttpClient):

    def request(self, url, method, params):
        if method not in ('GET', 'POST'):
            raise ArgumentError("method must be 'POST' or 'GET'")

        parse_result = urlparse(url)
        host = parse_result.hostname
        port = parse_result.port
        path = parse_result.path

        if parse_result.scheme == 'http':
            conn = httplib.HTTPConnection(host, port=port)
        else:
            conn = httplib.HTTPSConnection(host, port=port)

        try:
            req_url = path
            body = None
            headers = {}
            if method == 'GET':
                req_url = req_url + '?' + urllib.urlencode(params)
            else:
                body = urllib.urlencode(params)
                headers = {"Content-type": "application/x-www-form-urlencoded"}

            log.debug("[httplib] request url: %s, method: %s params: %s" % (url, method, params))
            conn.request(method, req_url, body, headers)
            response = conn.getresponse()
            http_status = response.status
            http_body = response.read()
            log.debug("[httplib] response status: %s body: %s" % (http_status, http_body))
        except httplib.HTTPException, e:
            raise HTTPError("httplib request exception: %s" % e.message)
        finally:
            conn.close()

        if http_status == httplib.OK:
            return http_body
        else:
            raise HTTPError("server http response error code: %s body: %s" % (http_status, http_body))


class RequestsHttpClient(HttpClient):

    def request(self, url, method, params):
        if method not in ('GET', 'POST'):
            raise ArgumentError("method must be 'POST' or 'GET'")

        try:
            log.debug("[requests] request url:%s method: %s params:%s" % (url, method, params))
            if method == 'GET':
                r = requests.get(url, params=params)
            else:
                r = requests.post(url, data=params)
            log.debug("[requests] response data:" + r.text)
        except requests.HTTPError, e:
            raise HTTPError("requests get exception: %s" % e.message)

        if r.status_code == 200:
            return r.text
        else:
            raise HTTPError("server http response code: %s" % r.status_code)
