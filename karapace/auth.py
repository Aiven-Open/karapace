from dataclasses import dataclass, field
from enum import Enum, unique
from hmac import compare_digest
from karapace.config import InvalidConfiguration
from karapace.rapu import JSON_CONTENT_TYPE
from typing import Any, Optional

import aiohttp
import aiohttp.web
import base64
import hashlib
import json
import logging
import re

log = logging.getLogger(__name__)


@unique
class Operation(Enum):
    Read = "Read"
    Write = "Write"


def hash_password(algorithm: str, salt: str, password: str) -> str:
    if algorithm != "scrypt":
        raise NotImplementedError(f"Hash algorithm '{algorithm}' is not implemented")
    return str(
        base64.b64encode(hashlib.scrypt(bytearray(password, "utf-8"), salt=bytearray(salt, "utf-8"), n=16384, r=8, p=1)),
        encoding="utf-8",
    )


@dataclass
class User:
    username: str
    algorithm: str
    salt: str
    password: str = field(repr=False)  # hashed

    def compare_password(self, plaintext: str) -> bool:
        return compare_digest(self.password, hash_password(self.algorithm, self.salt, plaintext))


@dataclass(frozen=True)
class ACL:
    username: str
    operation: Operation
    resource: re.Pattern


def http_authorizer(filename: str) -> Any:
    with open(filename, "r") as authfile:
        try:
            authdata = json.load(authfile)

            userdb = {user["username"]: User(**user) for user in authdata["users"]}
            acls = [
                ACL(acl["username"], Operation(acl["operation"]), re.compile(acl["resource"])) for acl in authdata["acls"]
            ]
            log.info(
                "Loaded schema registry ACL rules: %s",
                [(acl.username, acl.operation.value, acl.resource.pattern) for acl in acls],
            )
        except Exception as ex:
            raise InvalidConfiguration("Auth configuration is not a valid JSON") from ex

    def check_authorization(user: Optional[User], operation: Operation, resource: str) -> bool:
        def check_operation(operation: Operation, acl: ACL) -> bool:
            if operation == Operation.Read:
                return True
            if acl.operation == Operation.Write:
                return True
            return False

        def check_resource(resource: str, acl: ACL) -> bool:
            return acl.resource.match(resource) is not None

        for acl in acls:
            if (
                user is not None
                and acl.username == user.username
                and check_operation(operation, acl)
                and check_resource(resource, acl)
            ):
                return True
        return False

    def authorization_handler(operation: Operation, request: aiohttp.web.Request) -> None:
        user = None
        auth_header = request.headers.get("Authorization")
        if auth_header is not None:
            try:
                auth = aiohttp.BasicAuth.decode(auth_header)
                user = userdb.get(auth.login)
            except ValueError:
                # pylint: disable=raise-missing-from
                raise aiohttp.web.HTTPUnauthorized(
                    headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
                    text='{"message": "Unauthorized"}',
                    content_type=JSON_CONTENT_TYPE,
                )

            if user is None or not user.compare_password(auth.password):
                raise aiohttp.web.HTTPUnauthorized(
                    headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
                    text='{"message": "Unauthorized"}',
                    content_type=JSON_CONTENT_TYPE,
                )

        resource = (
            f"Subject:{request.match_info['subject']}" if request.match_info.get("subject") is not None else "Subject:"
        )
        if not check_authorization(user, operation, resource):
            raise aiohttp.web.HTTPForbidden(text='{"message": "Forbidden"}', content_type=JSON_CONTENT_TYPE)

    def context_authorizer(operation: Operation) -> Any:
        return lambda request: authorization_handler(operation, request)

    return context_authorizer


def no_authorizer(_: str) -> Any:
    return None
