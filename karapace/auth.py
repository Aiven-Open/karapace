from dataclasses import dataclass, field
from enum import Enum, unique
from hmac import compare_digest
from karapace.config import InvalidConfiguration
from karapace.rapu import JSON_CONTENT_TYPE
from typing import Optional

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


class HTTPAuthorizer:
    def __init__(self, filename: str):
        with open(filename, "r") as authfile:
            try:
                authdata = json.load(authfile)

                self.userdb = {user["username"]: User(**user) for user in authdata["users"]}
                self.acls = [
                    ACL(acl["username"], Operation(acl["operation"]), re.compile(acl["resource"]))
                    for acl in authdata["acls"]
                ]
                log.info(
                    "Loaded schema registry ACL rules: %s",
                    [(acl.username, acl.operation.value, acl.resource.pattern) for acl in self.acls],
                )
            except Exception as ex:
                raise InvalidConfiguration("Auth configuration is not a valid JSON") from ex

    def check_authorization(self, user: Optional[User], operation: Operation, resource: str) -> bool:
        def check_operation(operation: Operation, acl: ACL) -> bool:
            if operation == Operation.Read:
                return True
            if acl.operation == Operation.Write:
                return True
            return False

        def check_resource(resource: str, acl: ACL) -> bool:
            return acl.resource.match(resource) is not None

        for acl in self.acls:
            if (
                user is not None
                and acl.username == user.username
                and check_operation(operation, acl)
                and check_resource(resource, acl)
            ):
                return True
        return False

    def authenticate(self, request: aiohttp.web.Request) -> Optional[User]:
        user = None
        auth_header = request.headers.get("Authorization")
        if auth_header is not None:
            try:
                auth = aiohttp.BasicAuth.decode(auth_header)
                user = self.userdb.get(auth.login)
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

        return user
