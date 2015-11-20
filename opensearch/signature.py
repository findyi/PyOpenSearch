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
import urllib

from opensearch.exception import ArgumentError


class Signature(object):
    """
    """

    def sign(self, key, http_method, querys):
        canonicalized_query_string = self.canonicalize_query_string(querys)
        base_string = self.prepare_base_string(http_method, canonicalized_query_string)
        return self.hmac_sha1(key+'&', base_string)

    def canonicalize_query_string(self, querys):
        if not isinstance(querys, dict):
            raise ArgumentError("Invalid querys parameter. it must be a dict instance")

        querys.pop('Signature')

        sorted_querys = sorted(querys.iteritems(), key=lambda d: d[0])
        return urllib.urlencode(sorted_querys)

    def prepare_base_string(self, http_method, canonicalized_query_string):
        return "%s&%%2F&%s" % (http_method, urllib.quote(canonicalized_query_string))

    def hmac_sha1(self, key, raw):
        from hashlib import sha1
        import hmac
        import binascii

        hashed = hmac.new(key, raw, sha1)
        return binascii.b2a_base64(hashed.digest())[:-1]