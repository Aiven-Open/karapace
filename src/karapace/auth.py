"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from base64 import b64encode
from dataclasses import dataclass, field
from enum import Enum, unique
from hmac import compare_digest
from karapace.config import Config, InvalidConfiguration
from karapace.statsd import StatsClient
from karapace.utils import json_decode, json_encode
from typing import Protocol
from typing_extensions import override, TypedDict
from watchfiles import awatch, Change

import argparse
import asyncio
import base64
import hashlib
import logging
import os
import re
import secrets
import sys

log = logging.getLogger(__name__)


class AuthenticationError(Exception):
    pass


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


class UserData(TypedDict):
    username: str
    algorithm: str
    salt: str
    password_hash: str


class ACLEntryData(TypedDict):
    username: str
    operation: str
    resource: str


class AuthData(TypedDict):
    users: list[UserData]
    permissions: list[ACLEntryData]


class AuthenticateProtocol(Protocol):
    def authenticate(self, *, username: str, password: str) -> User | None: ...


class AuthorizeProtocol(Protocol):
    def get_user(self, username: str) -> User | None: ...

    def check_authorization(self, user: User | None, operation: Operation, resource: str) -> bool: ...

    def check_authorization_any(self, user: User | None, operation: Operation, resources: list[str]) -> bool: ...


class AuthenticatorAndAuthorizer(AuthenticateProtocol, AuthorizeProtocol):
    MUST_AUTHENTICATE: bool = True

    async def close(self) -> None: ...

    async def start(self, stats: StatsClient) -> None: ...


class NoAuthAndAuthz(AuthenticatorAndAuthorizer):
    MUST_AUTHENTICATE: bool = False

    @override
    def authenticate(self, *, username: str, password: str) -> User | None:
        return None

    @override
    def get_user(self, username: str) -> User | None:
        return None

    @override
    def check_authorization(self, user: User | None, operation: Operation, resource: str) -> bool:
        return True

    @override
    def check_authorization_any(self, user: User | None, operation: Operation, resources: list[str]) -> bool:
        return True

    @override
    async def close(self) -> None:
        pass

    @override
    async def start(self, stats: StatsClient) -> None:
        pass


class ACLAuthorizer(AuthorizeProtocol):
    def __init__(self, *, user_db: dict[str, User] | None = None, permissions: list[ACLEntry] | None = None) -> None:
        self.user_db = user_db or {}
        self.permissions = permissions or []

    def get_user(self, username: str) -> User | None:
        user = self.user_db.get(username)
        if not user:
            raise ValueError("No user found")
        return user

    def _check_resources(self, resources: list[str], aclentry: ACLEntry) -> bool:
        for resource in resources:
            if aclentry.resource.match(resource) is not None:
                return True
        return False

    def _check_operation(self, operation: Operation, aclentry: ACLEntry) -> bool:
        """Does ACL entry allow given operation.

        An entry at minimum gives Read permission. Write permission implies Read."""
        return operation == Operation.Read or aclentry.operation == Operation.Write

    @override
    def check_authorization(self, user: User | None, operation: Operation, resource: str) -> bool:
        if user is None:
            return False

        for aclentry in self.permissions:
            if (
                aclentry.username == user.username
                and self._check_operation(operation, aclentry)
                and self._check_resources([resource], aclentry)
            ):
                return True
        return False

    @override
    def check_authorization_any(self, user: User | None, operation: Operation, resources: list[str]) -> bool:
        """Checks that user is authorized to one of the resources in the list.

        If any resource in the list matches the permission the function returns True. This indicates only that
        one resource matches the permission and other resources may not.
        """
        if user is None:
            return False

        for aclentry in self.permissions:
            if (
                aclentry.username == user.username
                and self._check_operation(operation, aclentry)
                and self._check_resources(resources, aclentry)
            ):
                return True
        return False


class HTTPAuthorizer(ACLAuthorizer, AuthenticatorAndAuthorizer):
    def __init__(self, auth_file: str) -> None:
        super().__init__()
        self._auth_filename: str = auth_file
        self._auth_mtime: float = -1
        self._refresh_auth_task: asyncio.Task | None = None
        self._refresh_auth_awatch_stop_event = asyncio.Event()

    @property
    def authfile_last_modified(self) -> float:
        return self._auth_mtime

    @override
    async def start(self, stats: StatsClient) -> None:
        """Start authfile refresher task"""
        self._load_authfile()

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
                except Exception as ex:
                    log.exception("Schema registry auth file could not be loaded")
                    stats.unexpected_exception(ex=ex, where="schema_registry_authfile_reloader")
                    return

        self._refresh_auth_task = asyncio.create_task(_refresh_authfile())

    @override
    async def close(self) -> None:
        if self._refresh_auth_task is not None:
            self._refresh_auth_awatch_stop_event.set()
            self._refresh_auth_task.cancel()
            self._refresh_auth_task = None

    def _load_authfile(self) -> None:
        try:
            statinfo = os.stat(self._auth_filename)
            with open(self._auth_filename) as authfile:
                authdata = json_decode(authfile, AuthData)

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
                self.user_db = users
                log.info(
                    "Loaded schema registry users: %s",
                    users,
                )
                self.permissions = permissions
                log.info(
                    "Loaded schema registry access control rules: %s",
                    [(entry.username, entry.operation.value, entry.resource.pattern) for entry in permissions],
                )
            self._auth_mtime = statinfo.st_mtime
        except Exception as ex:
            raise InvalidConfiguration("Failed to load auth file") from ex

    @override
    def authenticate(self, *, username: str, password: str) -> User | None:
        user = self.get_user(username)
        if user is None or not user.compare_password(password):
            raise AuthenticationError()

        return user


def get_authorizer(
    config: Config,
    http_authorizer: HTTPAuthorizer,
    no_auth_authorizer: NoAuthAndAuthz,
) -> AuthenticatorAndAuthorizer:
    return http_authorizer if config.registry_authfile else no_auth_authorizer


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
