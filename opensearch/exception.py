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


class OpenSearchError(Exception):
    """Base exception used by opensearch module.
    """


class HTTPError(OpenSearchError):
    """HTTP request exception
    """


class ArgumentError(OpenSearchError):
    """Invalid argument value (or type)
    """


class QueryError(OpenSearchError):
    """Error in query statment
    """


class ApiError(OpenSearchError):
    """Server api response is not OK
    """

    def __init__(self, code, message):
        super(ApiError, self).__init__("api response code: %s message: %s" % (code, message))