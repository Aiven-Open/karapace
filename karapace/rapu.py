"""
karapace -
Custom middleware system on top of `aiohttp` implementing HTTP server and
client components for use in Aiven's REST applications.

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from accept_types import get_best_match
from http import HTTPStatus
from karapace.config import Config, create_server_ssl_context
from karapace.statsd import StatsClient
from karapace.utils import json_encode
from karapace.version import __version__
from typing import Dict, NoReturn, Optional, overload, Union

import aiohttp
import aiohttp.web
import aiohttp.web_exceptions
import asyncio
import cgi
import hashlib
import json
import logging
import re
import time

SERVER_NAME = "Karapace/{}".format(__version__)
JSON_CONTENT_TYPE = "application/json"

SCHEMA_CONTENT_TYPES = [
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry+json",
    JSON_CONTENT_TYPE,
    "application/octet-stream",
]
SCHEMA_ACCEPT_VALUES = [
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry+json",
    JSON_CONTENT_TYPE,
]

# TODO -> accept more general values as well
REST_CONTENT_TYPE_RE = re.compile(
    r"application/((vnd\.kafka(\.(?P<embedded_format>avro|json|protobuf|binary|jsonschema))?(\.(?P<api_version>v[12]))?"
    r"\+(?P<serialization_format>json))|(?P<general_format>json|octet-stream))"
)

REST_ACCEPT_RE = re.compile(
    r"(application|\*)/((vnd\.kafka(\.(?P<embedded_format>avro|json|"
    r"protobuf|binary|jsonschema))?(\.(?P<api_version>v[12]))?\+"
    r"(?P<serialization_format>json))|(?P<general_format>json|\*))"
)


def is_success(http_status: HTTPStatus) -> bool:
    """True if response has a 2xx status_code"""
    return http_status.value >= 200 and http_status.value < 300


class HTTPRequest:
    def __init__(
        self,
        *,
        url: str,
        query,
        headers: Dict[str, str],
        path_for_stats: str,
        method: str,
        content_type: Optional[str] = None,
        accepts: Optional[str] = None,
    ):
        self.url = url
        self.headers = headers
        self._header_cache: Dict[str, Optional[str]] = {}
        self.query = query
        self.content_type = content_type
        self.accepts = accepts
        self.path_for_stats = path_for_stats
        self.method = method
        self.json: Optional[dict] = None

    @overload
    def get_header(self, header: str) -> Optional[str]:
        ...

    @overload
    def get_header(self, header: str, default_value: str) -> str:
        ...

    def get_header(self, header, default_value=None):
        upper_cased = header.upper()
        if upper_cased in self._header_cache:
            return self._header_cache[upper_cased]
        for h in self.headers.keys():
            if h.upper() == upper_cased:
                value = self.headers[h]
                self._header_cache[upper_cased] = value
                return value
        if upper_cased == "CONTENT-TYPE":
            # sensible default
            self._header_cache[upper_cased] = JSON_CONTENT_TYPE
        else:
            self._header_cache[upper_cased] = default_value
        return self._header_cache[upper_cased]

    def __repr__(self):
        return "HTTPRequest(url=%s query=%s method=%s json=%r)" % (self.url, self.query, self.method, self.json)


class HTTPResponse(Exception):
    """A custom Response object derived from Exception so it can be raised
    in response handler callbacks."""

    status: HTTPStatus
    json: Union[None, list, dict]

    def __init__(
        self,
        body,
        *,
        status: HTTPStatus = HTTPStatus.OK,
        content_type: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.body = body
        self.status = status
        self.headers = dict(headers) if headers else {}

        if isinstance(body, (dict, list)):
            self.headers["Content-Type"] = JSON_CONTENT_TYPE
            self.json = body
        else:
            self.json = None
        if content_type:
            self.headers["Content-Type"] = content_type
        super().__init__(f"HTTPResponse {status.value}")

    def ok(self) -> bool:
        """True if resposne has a 2xx status_code"""
        return is_success(self.status)

    def __repr__(self) -> str:
        return f"HTTPResponse(status={self.status} body={self.body})"


def http_error(message, content_type: str, code: HTTPStatus) -> NoReturn:
    raise HTTPResponse(
        body=json_encode(
            {
                "error_code": code,
                "message": message,
            },
            binary=True,
        ),
        headers={"Content-Type": content_type},
        status=code,
    )


class RestApp:
    def __init__(self, *, app_name: str, config: Config) -> None:
        self.app_name = app_name
        self.config = config
        self.app_request_metric = "{}_request".format(app_name)
        self.app = aiohttp.web.Application()
        self.log = logging.getLogger(self.app_name)
        self.stats = StatsClient(config=config)
        self.app.on_cleanup.append(self.close_by_app)

    async def close_by_app(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
        await self.close()

    async def close(self) -> None:
        """Method used to free all the resources allocated by the applicaiton.

        This will be called as a callback by the aiohttp server. It needs to be
        set as hook because the awaitables have to run inside the event loop
        created by the aiohttp library.
        """
        self.stats.close()

    @staticmethod
    def cors_and_server_headers_for_request(*, request, origin="*"):  # pylint: disable=unused-argument
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": "DELETE, GET, OPTIONS, POST, PUT",
            "Access-Control-Allow-Headers": "Authorization, Content-Type",
            "Server": SERVER_NAME,
        }

    def check_rest_headers(self, request: HTTPRequest) -> dict:  # pylint:disable=inconsistent-return-statements
        method = request.method
        default_content = "application/vnd.kafka.json.v2+json"
        default_accept = "*/*"
        result = {"content_type": default_content}
        content_matcher = REST_CONTENT_TYPE_RE.search(
            cgi.parse_header(request.get_header("Content-Type", default_content))[0]
        )
        accept_matcher = REST_ACCEPT_RE.search(cgi.parse_header(request.get_header("Accept", default_accept))[0])
        if method in {"POST", "PUT"}:
            if not content_matcher:
                http_error(
                    message="HTTP 415 Unsupported Media Type",
                    content_type=result["content_type"],
                    code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                )
        if content_matcher and accept_matcher:
            header_info = content_matcher.groupdict()
            header_info["embedded_format"] = header_info.get("embedded_format") or "binary"
            result["requests"] = header_info
            result["accepts"] = accept_matcher.groupdict()
            return result
        self.log.error("Not acceptable: %r", request.get_header("accept"))
        http_error(
            message="HTTP 406 Not Acceptable",
            content_type=result["content_type"],
            code=HTTPStatus.NOT_ACCEPTABLE,
        )

    def check_schema_headers(self, request: HTTPRequest):
        method = request.method
        response_default_content_type = "application/vnd.schemaregistry.v1+json"
        content_type = request.get_header("Content-Type", JSON_CONTENT_TYPE)

        if method in {"POST", "PUT"} and cgi.parse_header(content_type)[0] not in SCHEMA_CONTENT_TYPES:
            http_error(
                message="HTTP 415 Unsupported Media Type",
                content_type=response_default_content_type,
                code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            )
        accept_val = request.get_header("Accept")
        if accept_val:
            if accept_val in ("*/*", "*") or accept_val.startswith("*/"):
                return response_default_content_type
            content_type_match = get_best_match(accept_val, SCHEMA_ACCEPT_VALUES)
            if not content_type_match:
                self.log.debug("Unexpected Accept value: %r", accept_val)
                http_error(
                    message="HTTP 406 Not Acceptable",
                    content_type=response_default_content_type,
                    code=HTTPStatus.NOT_ACCEPTABLE,
                )
            return content_type_match
        return response_default_content_type

    async def _handle_request(
        self,
        *,
        request,
        path_for_stats,
        callback,
        schema_request=False,
        callback_with_request=False,
        json_request=False,
        rest_request=False,
        user=None,
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
                    raise HTTPResponse(body="OPTIONS missing Origin", status=HTTPStatus.BAD_REQUEST)
                headers = self.cors_and_server_headers_for_request(request=rapu_request, origin=origin)
                raise HTTPResponse(body=b"", status=HTTPStatus.OK, headers=headers)

            body = await request.read()
            if json_request:
                if not body:
                    raise HTTPResponse(body="Missing request JSON body", status=HTTPStatus.BAD_REQUEST)
                try:
                    _, options = cgi.parse_header(rapu_request.get_header("Content-Type"))
                    charset = options.get("charset", "utf-8")
                    body_string = body.decode(charset)
                    rapu_request.json = json.loads(body_string)
                except UnicodeDecodeError:
                    raise HTTPResponse(  # pylint: disable=raise-missing-from
                        body=f"Request body is not valid {charset}", status=HTTPStatus.BAD_REQUEST
                    )
                except LookupError:
                    raise HTTPResponse(  # pylint: disable=raise-missing-from
                        body=f"Unknown charset {charset}", status=HTTPStatus.BAD_REQUEST
                    )
                except ValueError:
                    raise HTTPResponse(  # pylint: disable=raise-missing-from
                        body="Invalid request JSON body", status=HTTPStatus.BAD_REQUEST
                    )
            else:
                if body not in {b"", b"{}"}:
                    raise HTTPResponse(body="No request body allowed for this operation", status=HTTPStatus.BAD_REQUEST)

            callback_kwargs = dict(request.match_info)
            if callback_with_request:
                callback_kwargs["request"] = rapu_request

            if rest_request:
                params = self.check_rest_headers(rapu_request)
                if "requests" in params:
                    rapu_request.content_type = params["requests"]
                    params.pop("requests")
                if "accepts" in params:
                    rapu_request.accepts = params["accepts"]
                    params.pop("accepts")
                callback_kwargs.update(params)

            if schema_request:
                content_type = self.check_schema_headers(rapu_request)
                callback_kwargs["content_type"] = content_type

            if user is not None:
                callback_kwargs["user"] = user

            try:
                data = await callback(**callback_kwargs)
                status = HTTPStatus.OK
                headers = {}
            except HTTPResponse as ex:
                data = ex.body
                status = ex.status
                headers = ex.headers
            except:  # pylint: disable=bare-except
                self.log.exception("Internal server error")
                headers = {"Content-Type": "application/json"}
                data = {"error_code": HTTPStatus.INTERNAL_SERVER_ERROR.value, "message": "Internal server error"}
                status = HTTPStatus.INTERNAL_SERVER_ERROR
            headers.update(self.cors_and_server_headers_for_request(request=rapu_request))

            if isinstance(data, (dict, list)):
                resp_bytes = json_encode(data, binary=True)
            elif isinstance(data, str):
                if "Content-Type" not in headers:
                    headers["Content-Type"] = "text/plain; charset=utf-8"
                resp_bytes = data.encode("utf-8")
            else:
                resp_bytes = data

            # On 204 - NO CONTENT there is no point of calculating cache headers
            if is_success(status):
                if resp_bytes:
                    etag = '"{}"'.format(hashlib.md5(resp_bytes).hexdigest())
                else:
                    etag = '""'
                if_none_match = request.headers.get("if-none-match")
                if if_none_match and if_none_match.replace("W/", "") == etag:
                    status = HTTPStatus.NOT_MODIFIED
                    resp_bytes = b""

                headers["access-control-expose-headers"] = "etag"
                headers["etag"] = etag

            resp = aiohttp.web.Response(body=resp_bytes, status=status.value, headers=headers)
        except HTTPResponse as ex:
            if isinstance(ex.body, str):
                resp = aiohttp.web.Response(text=ex.body, status=ex.status.value, headers=ex.headers)
            else:
                resp = aiohttp.web.Response(body=ex.body, status=ex.status.value, headers=ex.headers)
        except aiohttp.web_exceptions.HTTPRequestEntityTooLarge:
            # This exception is not our usual http response, so to keep a consistent error interface
            # we construct http response manually here
            status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
            body = json_encode(
                {
                    "error_code": status,
                    "message": "HTTP Request Entity Too Large",
                },
                binary=True,
            )
            headers = {"Content-Type": "application/json"}
            resp = aiohttp.web.Response(body=body, status=status.value, headers=headers)
        except asyncio.CancelledError:
            self.log.debug("Client closed connection")
            raise
        except Exception as ex:  # pylint: disable=broad-except
            self.stats.unexpected_exception(ex=ex, where="rapu_wrapped_callback")
            self.log.exception("Unexpected error handling user request: %s %s", request.method, request.url)
            resp = aiohttp.web.Response(text="Internal Server Error", status=HTTPStatus.INTERNAL_SERVER_ERROR.value)
        finally:
            self.stats.timing(
                self.app_request_metric,
                time.monotonic() - start_time,
                tags={
                    "path": path_for_stats,
                    # no `resp` means that we had a failure in exception handler
                    "result": resp.status if resp else 0,
                    "method": request.method,
                },
            )

        return resp

    def route(
        self,
        path,
        *,
        callback,
        method,
        schema_request=False,
        with_request=None,
        json_body=None,
        rest_request=False,
        auth=None,
    ):
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
            if auth is not None:
                user = auth.authenticate(request)
            else:
                user = None

            return await self._handle_request(
                request=request,
                path_for_stats=path_for_stats,
                callback=callback,
                schema_request=schema_request,
                callback_with_request=with_request,
                json_request=json_body,
                rest_request=rest_request,
                user=user,
            )

        async def wrapped_cors(request):
            return await self._handle_request(
                request=request,
                path_for_stats=path_for_stats,
                callback=None,
            )

        if not aio_route.endswith("/"):
            self.app.router.add_route(method, aio_route + "/", wrapped_callback)
            self.app.router.add_route(method, aio_route, wrapped_callback)
        else:
            self.app.router.add_route(method, aio_route, wrapped_callback)
            self.app.router.add_route(method, aio_route[:-1], wrapped_callback)
        try:
            self.app.router.add_route("OPTIONS", aio_route, wrapped_cors)
        except RuntimeError as ex:
            if "Added route will never be executed, method OPTIONS is already registered" not in str(ex):
                raise

    def run(self) -> None:
        ssl_context = create_server_ssl_context(self.config)

        aiohttp.web.run_app(
            app=self.app,
            host=self.config["host"],
            port=self.config["port"],
            ssl_context=ssl_context,
            access_log_class=self.config["access_log_class"],
            access_log_format='%Tfs %{x-client-ip}i "%r" %s "%{user-agent}i" response=%bb request_body=%{content-length}ib',
        )

    def add_routes(self):
        pass  # Override in sub-classes
