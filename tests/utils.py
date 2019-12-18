"""
karapace - test utils

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import os
import requests


class Result:
    def __init__(self, status, json_result, headers=None):
        # We create both status and status_code so people can be agnostic on whichever to use
        self.status_code = status
        self.status = status
        self.json_result = json_result
        self.headers = headers if headers else {}

    def json(self):
        return self.json_result


class Client:
    def __init__(self, server_uri=None, client=None):
        self.server_uri = server_uri
        self.session = requests.Session()
        self.client = client

    async def get(self, path, headers=None):
        if not headers:
            headers = {}
        if self.client:
            async with self.client.get(
                path,
                headers=headers,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = requests.get(os.path.join(self.server_uri, path), headers=headers)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def delete(self, path, headers=None):
        if not headers:
            headers = {}
        if self.client:
            async with self.client.delete(
                path,
                headers=headers,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = requests.delete(os.path.join(self.server_uri, path), headers=headers)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def post(self, path, json, headers=None):
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        if self.client:
            async with self.client.post(
                path,
                headers=headers,
                json=json,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.post(os.path.join(self.server_uri, path), headers=headers, json=json)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def put(self, path, json, headers=None):
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        if self.client:
            async with self.client.put(
                path,
                headers=headers,
                json=json,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.put(os.path.join(self.server_uri, path), headers=headers, json=json)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)
