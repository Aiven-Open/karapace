"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from base64 import b64encode
from dataclasses import dataclass, field
from enum import Enum, unique
from hmac import compare_digest
from karapace.config import InvalidConfiguration
from karapace.rapu import JSON_CONTENT_TYPE
from karapace.statsd import StatsClient
from karapace.utils import json_decode, json_encode
from typing import Optional
from watchfiles import awatch, Change

import aiohttp
import aiohttp.web
import argparse
import asyncio
import base64
import hashlib
import logging
import re
import secrets
import sys

log = logging.getLogger(__name__)


@unique
class Operation(Enum):
    Read = "Read"
    Write = "Write"


@unique
class HashAlgorithm(Enum):
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    SCRYPT = "scrypt"


def hash_password(algorithm: HashAlgorithm, salt: str, plaintext_password: str) -> str:
    if algorithm in [HashAlgorithm.SHA1, HashAlgorithm.SHA256, HashAlgorithm.SHA512]:
        return b64encode(
            hashlib.pbkdf2_hmac(algorithm.value, bytearray(plaintext_password, "UTF-8"), bytearray(salt, "UTF-8"), 5000)
        ).decode("ascii")
    if algorithm == HashAlgorithm.SCRYPT:
        return str(
            base64.b64encode(
                hashlib.scrypt(bytearray(plaintext_password, "utf-8"), salt=bytearray(salt, "utf-8"), n=16384, r=8, p=1)
            ),
            encoding="utf-8",
        )
    raise NotImplementedError(f"Hash algorithm '{algorithm}' is not implemented")


@dataclass
class User:
    username: str
    algorithm: HashAlgorithm
    salt: str
    password_hash: str = field(repr=False)

    def compare_password(self, plaintext_password: str) -> bool:
        return compare_digest(self.password_hash, hash_password(self.algorithm, self.salt, plaintext_password))


@dataclass(frozen=True)
class ACLEntry:
    username: str
    operation: Operation
    resource: re.Pattern


class HTTPAuthorizer:
    def __init__(self, filename: str) -> None:
        self._auth_filename: str = filename
        self._refresh_auth_task: Optional[asyncio.Task] = None
        self._refresh_auth_awatch_stop_event = asyncio.Event()
        # Once first, can raise if file not valid
        self._load_authfile()

    async def start_refresh_task(self, stats: StatsClient) -> None:
        """Start authfile refresher task"""

        async def _refresh_authfile() -> None:
            """Reload authfile, but keep old auth data if loading fails"""
            while True:
                try:
                    async for changes in awatch(self._auth_filename, stop_event=self._refresh_auth_awatch_stop_event):
                        try:
                            self._load_authfile()
                        except InvalidConfiguration as e:
                            log.warning("Could not load authentication file: %s", e)

                        if Change.deleted in {change for change, _ in changes}:
                            # Reset watch after delete event (e.g. file is replaced)
                            break
                except asyncio.CancelledError:
                    log.info("Closing schema registry ACL refresh task")
                    return
                except Exception as ex:  # pylint: disable=broad-except
                    log.exception("Schema registry auth file could not be loaded")
                    stats.unexpected_exception(ex=ex, where="schema_registry_authfile_reloader")
                    return

        self._refresh_auth_task = asyncio.create_task(_refresh_authfile())

    async def close(self) -> None:
        if self._refresh_auth_task is not None:
            self._refresh_auth_awatch_stop_event.set()
            self._refresh_auth_task.cancel()
            self._refresh_auth_task = None

    def _load_authfile(self) -> None:
        try:
            with open(self._auth_filename) as authfile:
                authdata = json_decode(authfile)

                users = {
                    user["username"]: User(
                        username=user["username"],
                        algorithm=HashAlgorithm(user["algorithm"]),
                        salt=user["salt"],
                        password_hash=user["password_hash"],
                    )
                    for user in authdata["users"]
                }
                permissions = [
                    ACLEntry(entry["username"], Operation(entry["operation"]), re.compile(entry["resource"]))
                    for entry in authdata["permissions"]
                ]
                self.userdb = users
                log.info(
                    "Loaded schema registry users: %s",
                    users,
                )
                self.permissions = permissions
                log.info(
                    "Loaded schema registry access control rules: %s",
                    [(entry.username, entry.operation.value, entry.resource.pattern) for entry in permissions],
                )
        except Exception as ex:
            raise InvalidConfiguration("Failed to load auth file") from ex

    def check_authorization(self, user: Optional[User], operation: Operation, resource: str) -> bool:
        if user is None:
            return False

        def check_operation(operation: Operation, aclentry: ACLEntry) -> bool:
            """Does ACL entry allow given operation.

            An entry at minimum gives Read permission. Write permission implies Read."""
            return operation == Operation.Read or aclentry.operation == Operation.Write

        def check_resource(resource: str, aclentry: ACLEntry) -> bool:
            return aclentry.resource.match(resource) is not None

        for aclentry in self.permissions:
            if (
                aclentry.username == user.username
                and check_operation(operation, aclentry)
                and check_resource(resource, aclentry)
            ):
                return True
        return False

    def authenticate(self, request: aiohttp.web.Request) -> User:
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            raise aiohttp.web.HTTPUnauthorized(
                headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
                text='{"message": "Unauthorized"}',
                content_type=JSON_CONTENT_TYPE,
            )
        try:
            auth = aiohttp.BasicAuth.decode(auth_header)
        except ValueError:
            # pylint: disable=raise-missing-from
            raise aiohttp.web.HTTPUnauthorized(
                headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
                text='{"message": "Unauthorized"}',
                content_type=JSON_CONTENT_TYPE,
            )
        user = self.userdb.get(auth.login)
        if user is None or not user.compare_password(auth.password):
            raise aiohttp.web.HTTPUnauthorized(
                headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
                text='{"message": "Unauthorized"}',
                content_type=JSON_CONTENT_TYPE,
            )

        return user


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace_mkpasswd", description="Karapace password hasher")
    parser.add_argument("-u", "--user", help="Username", type=str)
    parser.add_argument(
        "-a", "--algorithm", help="Hash algorithm", choices=["sha1", "sha256", "sha512", "scrypt"], default="sha512"
    )
    parser.add_argument(metavar="password", dest="plaintext_password", help="Password to hash", type=str)
    parser.add_argument("salt", help="Salt for hashing, random generated if not given", nargs="?", type=str)
    args = parser.parse_args()
    salt: str = args.salt or secrets.token_urlsafe(nbytes=16)
    result = {}
    if args.user:
        result["username"] = args.user
    result["algorithm"] = args.algorithm
    result["salt"] = salt
    result["password_hash"] = hash_password(HashAlgorithm(args.algorithm), salt, args.plaintext_password)

    print(json_encode(result, compact=False, indent=4))
    return 0


if __name__ == "__main__":
    sys.exit(main())
