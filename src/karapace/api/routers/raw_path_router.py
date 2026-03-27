"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

"""
The MIT License (MIT)

Copyright (c) 2018 Sebastián Ramírez
"""

from collections.abc import Coroutine, Callable  # noqa: E402
from fastapi import HTTPException, Request, Response  # noqa: E402
from typing import Any  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from fastapi.routing import APIRoute  # noqa: E402
from karapace.api.content_type import negotiate_schema_content_type, SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE  # noqa: E402
from starlette.routing import Match  # noqa: E402
from starlette.types import Scope  # noqa: E402

import re  # noqa: E402


class RawPathRoute(APIRoute):
    """The subject in the paths can contain url encoded forward slash

    See explanation and origin of the solution:
     - https://github.com/fastapi/fastapi/discussions/7328#discussioncomment-8443865
    Starlette defect discussion:
     - https://github.com/encode/starlette/issues/826
    """

    def matches(self, scope: Scope) -> tuple[Match, Scope]:
        raw_path: str | None = None

        if "raw_path" in scope and scope["raw_path"] is not None:
            raw_path = scope["raw_path"].decode("utf-8")

        if raw_path is None:
            raise HTTPException(status_code=500, detail="Internal routing error")

        # Drop the last forward slash if present. e.g. '/path/' -> '/path', but from path '/'
        if len(raw_path) > 1:
            raw_path = raw_path if raw_path[-1] != "/" else raw_path[:-1]

        new_path = re.sub(r"\?.*", "", raw_path)
        scope["path"] = new_path
        return super().matches(scope)


class SchemaRegistryRoute(RawPathRoute):
    """Route class for schema-registry endpoints that require content negotiation.

    Validates Accept and Content-Type headers before any dependency injection or
    body parsing occurs. Returns 406/415 errors immediately for invalid headers,
    and sets the negotiated Content-Type on successful responses.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_handler = super().get_route_handler()

        async def schema_content_handler(request: Request) -> Response:
            try:
                response_content_type = negotiate_schema_content_type(request)
            except HTTPException as exc:
                return JSONResponse(
                    status_code=exc.status_code,
                    content=exc.detail,
                    headers={"Content-Type": SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE},
                )

            if request.headers.get("Content-Type") == "application/octet-stream":
                new_headers = request.headers.mutablecopy()
                new_headers["Content-Type"] = "application/json"
                request._headers = new_headers
                request.scope.update(headers=request.headers.raw)

            request.state.schema_response_content_type = response_content_type

            response = await original_handler(request)
            response.headers["Content-Type"] = response_content_type
            return response

        return schema_content_handler
