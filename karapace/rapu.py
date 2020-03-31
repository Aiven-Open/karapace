"""
karapace -
Custom middleware system on top of `aiohttp` implementing HTTP server and
client components for use in Aiven's REST applications.

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.statsd import StatsClient
from karapace.utils import json_encode
from karapace.version import __version__
from vendor.python_accept_types.accept_types import get_best_match

import aiohttp
import aiohttp.web
import aiohttp_socks
import async_timeout
import asyncio
import hashlib
import json as jsonlib
import logging
import re
import ssl
import time

SERVER_NAME = "Karapace/{}".format(__version__)

SCHEMA_CONTENT_TYPES = [
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry+json",
    "application/json",
    "application/octet-stream",
]
SCHEMA_ACCEPT_VALUES = [
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry+json",
    "application/json",
]


class HTTPRequest:
    def __init__(self, *, url, query, headers, path_for_stats, method):
        self.url = url
        self.headers = headers
        self.query = query
        self.path_for_stats = path_for_stats
        self.method = method
        self.json = None


class HTTPResponse(Exception):
    """A custom Response object derived from Exception so it can be raised
    in response handler callbacks."""

    def __init__(self, body, *, status=200, content_type=None, headers=None):
        self.body = body
        self.status = status
        self.headers = dict(headers) if headers else {}
        if isinstance(body, (dict, list)):
            self.headers["Content-Type"] = "application/json"
            self.json = body
        else:
            self.json = None
        if content_type:
            self.headers["Content-Type"] = content_type
        super().__init__("HTTPResponse {}".format(status))

    def ok(self):
        if self.status < 200 or self.status >= 300:
            return False
        return True


class RestApp:
    def __init__(self, *, app_name, sentry_config):
        self.app_name = app_name
        self.app_request_metric = "{}_request".format(app_name)
        self.app = aiohttp.web.Application()
        self.app.on_startup.append(self.create_http_client)
        self.app.on_cleanup.append(self.cleanup_http_client)
        self.http_client_v = None
        self.http_client_no_v = None
        self.log = logging.getLogger(self.app_name)
        self.stats = StatsClient(sentry_config=sentry_config)
        self.raven_client = self.stats.raven_client
        self.app.on_cleanup.append(self.cleanup_stats_client)

    async def cleanup_stats_client(self, app):  # pylint: disable=unused-argument
        self.stats.close()

    async def create_http_client(self, app):  # pylint: disable=unused-argument
        no_v_conn = aiohttp.TCPConnector(verify_ssl=False)
        self.http_client_no_v = aiohttp.ClientSession(connector=no_v_conn, headers={"User-Agent": SERVER_NAME})
        self.http_client_v = aiohttp.ClientSession(headers={"User-Agent": SERVER_NAME})

    async def cleanup_http_client(self, app):  # pylint: disable=unused-argument
        if self.http_client_no_v:
            await self.http_client_no_v.close()
        if self.http_client_v:
            await self.http_client_v.close()

    @staticmethod
    def cors_and_server_headers_for_request(*, request, origin="*"):  # pylint: disable=unused-argument
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": "DELETE, GET, OPTIONS, POST, PUT",
            "Access-Control-Allow-Headers": "Authorization, Content-Type",
            "Server": SERVER_NAME,
        }

    def check_schema_headers(self, request):
        method = request.method
        headers = request.headers

        content_type = "application/vnd.schemaregistry.v1+json"
        if method in {"POST", "PUT"} and headers["Content-Type"] not in SCHEMA_CONTENT_TYPES:
            raise HTTPResponse(
                body=json_encode({
                    "error_code": 415,
                    "message": "HTTP 415 Unsupported Media Type",
                }, binary=True),
                headers={"Content-Type": content_type},
                status=415,
            )

        if "Accept" in headers:
            if headers["Accept"] == "*/*" or headers["Accept"].startswith("*/"):
                return "application/vnd.schemaregistry.v1+json"
            content_type_match = get_best_match(headers["Accept"], SCHEMA_ACCEPT_VALUES)
            if not content_type_match:
                self.log.debug("Unexpected Accept value: %r", headers["Accept"])
                raise HTTPResponse(
                    body=json_encode({
                        "error_code": 406,
                        "message": "HTTP 406 Not Acceptable",
                    }, binary=True),
                    headers={"Content-Type": content_type},
                    status=406,
                )
            return content_type_match
        return content_type

    async def _handle_request(
        self, *, request, path_for_stats, callback, schema_request=False, callback_with_request=False, json_request=False
    ):
        start_time = time.monotonic()
        resp = None
        rapu_request = HTTPRequest(
            headers=request.headers,
            query=request.query,
            method=request.method,
            url=request.url,
            path_for_stats=path_for_stats,
        )
        try:
            if request.method == "OPTIONS":
                origin = request.headers.get("Origin")
                if not origin:
                    raise HTTPResponse(body="OPTIONS missing Origin", status=400)
                headers = self.cors_and_server_headers_for_request(request=rapu_request, origin=origin)
                raise HTTPResponse(body=b"", status=200, headers=headers)

            body = await request.read()
            if json_request:
                if not body:
                    raise HTTPResponse(body="Missing request JSON body", status=400)
                if request.charset and request.charset.lower() != "utf-8" and request.charset.lower() != "utf8":
                    raise HTTPResponse(body="Request character set must be UTF-8", status=400)
                try:
                    body_string = body.decode("utf-8")
                    rapu_request.json = jsonlib.loads(body_string)
                except jsonlib.decoder.JSONDecodeError:
                    raise HTTPResponse(body="Invalid request JSON body", status=400)
                except UnicodeDecodeError:
                    raise HTTPResponse(body="Request body is not valid UTF-8", status=400)
            else:
                if body not in {b"", b"{}"}:
                    raise HTTPResponse(body="No request body allowed for this operation", status=400)

            callback_kwargs = dict(request.match_info)
            if callback_with_request:
                callback_kwargs["request"] = rapu_request

            if schema_request:
                content_type = self.check_schema_headers(request)
                callback_kwargs["content_type"] = content_type

            try:
                data = await callback(**callback_kwargs)
                status = 200
                headers = {}
            except HTTPResponse as ex:
                data = ex.body
                status = ex.status
                headers = ex.headers
            headers.update(self.cors_and_server_headers_for_request(request=rapu_request))

            if isinstance(data, (dict, list)):
                resp_bytes = json_encode(data, binary=True, sort_keys=True, compact=True)
            elif isinstance(data, str):
                if "Content-Type" not in headers:
                    headers["Content-Type"] = "text/plain; charset=utf-8"
                resp_bytes = data.encode("utf-8")
            else:
                resp_bytes = data

            # On 204 - NO CONTENT there is no point of calculating cache headers
            if 200 >= status <= 299:
                if resp_bytes:
                    etag = '"{}"'.format(hashlib.md5(resp_bytes).hexdigest())
                else:
                    etag = '""'
                if_none_match = request.headers.get("if-none-match")
                if if_none_match and if_none_match.replace("W/", "") == etag:
                    status = 304
                    resp_bytes = b""

                headers["access-control-expose-headers"] = "etag"
                headers["etag"] = etag

            resp = aiohttp.web.Response(body=resp_bytes, status=status, headers=headers)
        except HTTPResponse as ex:
            if isinstance(ex.body, str):
                resp = aiohttp.web.Response(text=ex.body, status=ex.status, headers=ex.headers)
            else:
                resp = aiohttp.web.Response(body=ex.body, status=ex.status, headers=ex.headers)
        except asyncio.CancelledError:
            self.log.debug("Client closed connection")
            raise
        except Exception as ex:  # pylint: disable=broad-except
            self.stats.unexpected_exception(ex=ex, where="rapu_wrapped_callback")
            self.log.exception("Unexpected error handling user request: %s %s", request.method, request.url)
            resp = aiohttp.web.Response(text="Internal Server Error", status=500)
        finally:
            self.stats.timing(
                self.app_request_metric,
                time.monotonic() - start_time,
                tags={
                    "path": path_for_stats,
                    # no `resp` means that we had a failure in exception handler
                    "result": resp.status if resp else 0,
                    "method": request.method,
                }
            )

        return resp

    def route(self, path, *, callback, method, schema_request=False, with_request=None, json_body=None):
        # pretty path for statsd reporting
        path_for_stats = re.sub(r"<[\w:]+>", "x", path)

        # bottle compatible routing
        aio_route = path
        aio_route = re.sub(r"<(\w+):path>", r"{\1:.+}", aio_route)
        aio_route = re.sub(r"<(\w+)>", r"{\1}", aio_route)

        if (method in {"POST", "PUT"}) and with_request is None:
            with_request = True

        if with_request and json_body is None:
            json_body = True

        async def wrapped_callback(request):
            return await self._handle_request(
                request=request,
                path_for_stats=path_for_stats,
                callback=callback,
                schema_request=schema_request,
                callback_with_request=with_request,
                json_request=json_body,
            )

        async def wrapped_cors(request):
            return await self._handle_request(
                request=request,
                path_for_stats=path_for_stats,
                callback=None,
            )

        self.app.router.add_route(method, aio_route, wrapped_callback)
        try:
            self.app.router.add_route("OPTIONS", aio_route, wrapped_cors)
        except RuntimeError as ex:
            if "Added route will never be executed, method OPTIONS is already registered" not in str(ex):
                raise

    async def http_request(self, url, *, method="GET", json=None, timeout=10.0, verify=True, proxy=None):
        close_session = False

        if isinstance(verify, str):
            sslcontext = ssl.create_default_context(cadata=verify)
        else:
            sslcontext = None

        if proxy:
            connector = aiohttp_socks.SocksConnector(
                socks_ver=aiohttp_socks.SocksVer.SOCKS5,
                host=proxy["host"],
                port=proxy["port"],
                username=proxy["username"],
                password=proxy["password"],
                rdns=False,
                verify_ssl=verify,
                ssl_context=sslcontext,
            )
            session = aiohttp.ClientSession(connector=connector)
            close_session = True
        elif sslcontext:
            conn = aiohttp.TCPConnector(ssl_context=sslcontext)
            session = aiohttp.ClientSession(connector=conn)
            close_session = True
        elif verify is True:
            session = self.http_client_v
        elif verify is False:
            session = self.http_client_no_v
        else:
            raise ValueError("invalid arguments to http_request")

        func = getattr(session, method.lower())
        try:
            with async_timeout.timeout(timeout):
                async with func(url, json=json) as response:
                    if response.headers.get("content-type", "").startswith("application/json"):
                        resp_content = await response.json()
                    else:
                        resp_content = await response.text()
                    result = HTTPResponse(body=resp_content, status=response.status)
        finally:
            if close_session:
                await session.close()

        return result

    def run(self, *, host, port):
        aiohttp.web.run_app(
            app=self.app,
            host=host,
            port=port,
            access_log_format='%Tfs %{x-client-ip}i "%r" %s "%{user-agent}i" response=%bb request_body=%{content-length}ib',
        )

    def add_routes(self):
        pass  # Override in sub-classes
