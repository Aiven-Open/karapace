"""
karapace - test utils

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.utils import Result

import os
import requests


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
