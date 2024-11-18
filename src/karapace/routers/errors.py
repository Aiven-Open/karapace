"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from enum import Enum, unique
from fastapi import HTTPException, status
from fastapi.exceptions import RequestValidationError


@unique
class SchemaErrorCodes(Enum):
    HTTP_BAD_REQUEST = status.HTTP_400_BAD_REQUEST
    HTTP_NOT_FOUND = status.HTTP_404_NOT_FOUND
    HTTP_CONFLICT = status.HTTP_409_CONFLICT
    HTTP_UNPROCESSABLE_ENTITY = status.HTTP_422_UNPROCESSABLE_ENTITY
    HTTP_INTERNAL_SERVER_ERROR = status.HTTP_500_INTERNAL_SERVER_ERROR
    SUBJECT_NOT_FOUND = 40401
    VERSION_NOT_FOUND = 40402
    SCHEMA_NOT_FOUND = 40403
    SUBJECT_SOFT_DELETED = 40404
    SUBJECT_NOT_SOFT_DELETED = 40405
    SCHEMAVERSION_SOFT_DELETED = 40406
    SCHEMAVERSION_NOT_SOFT_DELETED = 40407
    SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE = 40408
    INVALID_VERSION_ID = 42202
    INVALID_COMPATIBILITY_LEVEL = 42203
    INVALID_SCHEMA = 42201
    INVALID_SUBJECT = 42208
    SCHEMA_TOO_LARGE_ERROR_CODE = 42209
    REFERENCES_SUPPORT_NOT_IMPLEMENTED = 44302
    REFERENCE_EXISTS = 42206
    NO_MASTER_ERROR = 50003


class KarapaceValidationError(RequestValidationError):
    def __init__(self, error_code: int, error: str):
        super().__init__(errors=[], body=error)
        self.error_code = error_code


def no_primary_url_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail={
            "error_code": SchemaErrorCodes.NO_MASTER_ERROR,
            "message": "Error while forwarding the request to the master.",
        },
    )
