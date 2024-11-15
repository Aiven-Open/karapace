"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from accept_types import get_best_match
from email.message import Message
from fastapi import HTTPException, Request, status

import logging


LOG = logging.getLogger(__name__)

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


def check_schema_headers(request: Request) -> str:
    method = request.method
    response_default_content_type = "application/vnd.schemaregistry.v1+json"

    message = Message()
    message["Content-Type"] = request.headers.get("Content-Type", JSON_CONTENT_TYPE)
    params = message.get_params()
    assert params is not None
    content_type = params[0][0]

    if method in {"POST", "PUT"} and content_type not in SCHEMA_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail={
                "message": "HTTP 415 Unsupported Media Type",
            },
            headers={
                "Content-Type": response_default_content_type,
            },
        )
    accept_val = request.headers.get("Accept")
    if accept_val:
        if accept_val in ("*/*", "*") or accept_val.startswith("*/"):
            return response_default_content_type
        content_type_match = get_best_match(accept_val, SCHEMA_ACCEPT_VALUES)
        if not content_type_match:
            LOG.debug("Unexpected Accept value: %r", accept_val)
            raise HTTPException(
                status_code=status.HTTP_406_NOT_ACCEPTABLE,
                detail={
                    "message": "HTTP 406 Not Acceptable",
                },
                headers={
                    "Content-Type": response_default_content_type,
                },
            )
        return content_type_match
    return response_default_content_type
