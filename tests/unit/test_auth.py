"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import asyncio
import json
import re
import sys
from unittest.mock import MagicMock, patch

import pytest
from watchfiles import Change

from karapace.core.auth import (
    ACLAuthorizer,
    ACLEntry,
    AuthenticationError,
    HashAlgorithm,
    HTTPAuthorizer,
    NoAuthAndAuthz,
    Operation,
    User,
    get_authorizer,
    hash_password,
    main,
)
from karapace.core.config import Config, InvalidConfiguration
from karapace.core.stats import StatsClient


def test_empty_acl_authorizer() -> None:
    authorizer = ACLAuthorizer()
    admin_password_hash = hash_password(algorithm=HashAlgorithm.SHA256, salt="salt", plaintext_password="password")
    assert False is authorizer.check_authorization(
        user=User(username="admin", algorithm=HashAlgorithm.SHA256, salt="salt", password_hash=admin_password_hash),
        operation=Operation.Read,
        resource="Subject:*",
    )


def test_acl_authorizer() -> None:
    admin_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="admin_password",
    )
    read_user_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="read_password",
    )
    write_user_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="write_password",
    )
    readwrite_user_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="readwrite_password",
    )

    admin_user = User(
        username="admin",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=admin_password_hash,
    )
    read_user = User(
        username="read",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=read_user_password_hash,
    )
    write_user = User(
        username="write",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=write_user_password_hash,
    )
    readwrite_user = User(
        username="readwrite",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=readwrite_user_password_hash,
    )

    authorizer = ACLAuthorizer(
        user_db={
            "admin": admin_user,
            "read": read_user,
            "write": write_user,
            "readwrite": readwrite_user,
        },
        permissions=[
            ACLEntry("admin", Operation.Read, re.compile(r"Subject:.*")),
            ACLEntry("admin", Operation.Write, re.compile(r"Subject:.*")),
            ACLEntry("admin", Operation.Read, re.compile(r"Config:.*")),
            ACLEntry("admin", Operation.Write, re.compile(r"Config:.*")),
            ACLEntry("read", Operation.Read, re.compile(r"Subject:read_subject")),
            ACLEntry("write", Operation.Write, re.compile(r"Subject:write_subject")),
            ACLEntry("readwrite", Operation.Read, re.compile(r"Subject:readwrite_subject")),
            ACLEntry("readwrite", Operation.Write, re.compile(r"Subject:readwrite_subject")),
        ],
    )

    assert True is authorizer.check_authorization(
        user=admin_user,
        operation=Operation.Read,
        resource="Subject:any_subject",
    )
    assert True is authorizer.check_authorization(
        user=admin_user,
        operation=Operation.Read,
        resource="Config:any_config",
    )
    assert True is authorizer.check_authorization_any(
        user=admin_user,
        operation=Operation.Read,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )

    assert True is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Read,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Read,
        resource="Subject:any_subject",
    )
    assert False is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Write,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Write,
        resource="Subject:write_subject",
    )
    assert False is authorizer.check_authorization_any(
        user=read_user,
        operation=Operation.Read,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )
    assert True is authorizer.check_authorization_any(
        user=read_user,
        operation=Operation.Read,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
            "Subject:read_subject",
        ],
    )

    assert True is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Write,
        resource="Subject:write_subject",
    )
    assert False is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Write,
        resource="Subject:any_subject",
    )
    assert False is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Write,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Read,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization_any(
        user=write_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )
    assert True is authorizer.check_authorization_any(
        user=write_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
            "Subject:write_subject",
        ],
    )

    assert True is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Write,
        resource="Subject:readwrite_subject",
    )
    assert True is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Read,
        resource="Subject:readwrite_subject",
    )
    assert False is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Write,
        resource="Subject:any_subject",
    )
    assert False is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Write,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization_any(
        user=readwrite_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )
    assert True is authorizer.check_authorization_any(
        user=readwrite_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
            "Subject:readwrite_subject",
        ],
    )


def test_get_user_returns_none_for_nonexistent_user() -> None:
    """get_user must return None (not raise) for unknown usernames.

    Regression test: a previous implementation raised ValueError here,
    which bypassed the AuthenticationError handling in authenticate()
    and surfaced as an unhandled 500 to clients.
    """
    admin_password_hash = hash_password(algorithm=HashAlgorithm.SHA256, salt="salt", plaintext_password="password")
    authorizer = ACLAuthorizer(
        user_db={
            "admin": User(username="admin", algorithm=HashAlgorithm.SHA256, salt="salt", password_hash=admin_password_hash),
        },
    )

    assert authorizer.get_user("admin") is not None
    assert authorizer.get_user("nonexistent") is None


def test_authenticate_raises_authentication_error_for_nonexistent_user(tmp_path) -> None:
    """authenticate() must raise AuthenticationError -- not ValueError --
    when the user does not exist, so the caller can return a proper 401."""
    admin_password_hash = hash_password(algorithm=HashAlgorithm.SHA256, salt="salt", plaintext_password="password")
    auth_file = _make_authfile(
        tmp_path,
        users=[{"username": "admin", "algorithm": "sha256", "salt": "salt", "password_hash": admin_password_hash}],
    )
    http_authorizer = HTTPAuthorizer(auth_file)
    http_authorizer._load_authfile()

    with pytest.raises(AuthenticationError):
        http_authorizer.authenticate(username="nonexistent", password="any")


def test_authenticate_raises_authentication_error_for_wrong_password(tmp_path) -> None:
    admin_password_hash = hash_password(algorithm=HashAlgorithm.SHA256, salt="salt", plaintext_password="password")
    auth_file = _make_authfile(
        tmp_path,
        users=[{"username": "admin", "algorithm": "sha256", "salt": "salt", "password_hash": admin_password_hash}],
    )
    http_authorizer = HTTPAuthorizer(auth_file)
    http_authorizer._load_authfile()

    with pytest.raises(AuthenticationError):
        http_authorizer.authenticate(username="admin", password="wrong")


class TestHashPassword:
    def test_scrypt_algorithm_produces_stable_hash(self) -> None:
        first = hash_password(HashAlgorithm.SCRYPT, salt="salt", plaintext_password="password")
        second = hash_password(HashAlgorithm.SCRYPT, salt="salt", plaintext_password="password")
        assert first == second

    def test_unsupported_algorithm_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError, match="not implemented"):
            hash_password(None, salt="salt", plaintext_password="password")  # type: ignore[arg-type]


class TestNoAuthAndAuthz:
    def test_authenticate_returns_none(self) -> None:
        assert NoAuthAndAuthz().authenticate(username="anyone", password="anything") is None

    def test_get_user_returns_none(self) -> None:
        assert NoAuthAndAuthz().get_user("anyone") is None

    def test_check_authorization_always_true(self) -> None:
        assert NoAuthAndAuthz().check_authorization(user=None, operation=Operation.Write, resource="Subject:*") is True

    def test_check_authorization_any_always_true(self) -> None:
        assert NoAuthAndAuthz().check_authorization_any(user=None, operation=Operation.Read, resources=["Subject:*"]) is True

    async def test_close_is_a_noop(self) -> None:
        await NoAuthAndAuthz().close()

    async def test_start_is_a_noop(self) -> None:
        await NoAuthAndAuthz().start(stats=MagicMock(spec=StatsClient))


class TestACLAuthorizerNoneUser:
    """A `None` user (unauthenticated caller) must always be denied, never matched."""

    def test_check_authorization_denies_none_user(self) -> None:
        authorizer = ACLAuthorizer(permissions=[ACLEntry("admin", Operation.Read, re.compile("Subject:.*"))])
        assert authorizer.check_authorization(user=None, operation=Operation.Read, resource="Subject:s") is False

    def test_check_authorization_any_denies_none_user(self) -> None:
        authorizer = ACLAuthorizer(permissions=[ACLEntry("admin", Operation.Read, re.compile("Subject:.*"))])
        assert authorizer.check_authorization_any(user=None, operation=Operation.Read, resources=["Subject:s"]) is False


def _make_authfile(tmp_path, *, users=None, permissions=None):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(json.dumps({"users": users or [], "permissions": permissions or []}))
    return str(auth_file)


class TestHTTPAuthorizerConstruction:
    def test_init_sets_defaults(self, tmp_path) -> None:
        auth_file = _make_authfile(tmp_path)
        authorizer = HTTPAuthorizer(auth_file)

        assert authorizer._auth_filename == auth_file
        assert authorizer.authfile_last_modified == -1
        assert authorizer._refresh_auth_task is None


class TestLoadAuthfile:
    def test_loads_users_and_permissions_from_disk(self, tmp_path) -> None:
        auth_file = _make_authfile(
            tmp_path,
            users=[{"username": "admin", "algorithm": "sha256", "salt": "s", "password_hash": "h"}],
            permissions=[{"username": "admin", "operation": "Read", "resource": "Subject:.*"}],
        )
        authorizer = HTTPAuthorizer(auth_file)

        authorizer._load_authfile()

        assert set(authorizer.user_db) == {"admin"}
        assert authorizer.user_db["admin"].algorithm == HashAlgorithm.SHA256
        assert len(authorizer.permissions) == 1
        assert authorizer.permissions[0].operation == Operation.Read
        assert authorizer.authfile_last_modified > 0

    def test_missing_file_raises_invalid_configuration(self, tmp_path) -> None:
        authorizer = HTTPAuthorizer(str(tmp_path / "does-not-exist.json"))

        with pytest.raises(InvalidConfiguration):
            authorizer._load_authfile()

    def test_malformed_json_raises_invalid_configuration(self, tmp_path) -> None:
        auth_file = tmp_path / "auth.json"
        auth_file.write_text("not json")
        authorizer = HTTPAuthorizer(str(auth_file))

        with pytest.raises(InvalidConfiguration):
            authorizer._load_authfile()

    def test_authenticate_returns_user_on_success(self, tmp_path) -> None:
        password_hash = hash_password(HashAlgorithm.SHA256, salt="s", plaintext_password="secret")
        auth_file = _make_authfile(
            tmp_path,
            users=[{"username": "admin", "algorithm": "sha256", "salt": "s", "password_hash": password_hash}],
        )
        authorizer = HTTPAuthorizer(auth_file)
        authorizer._load_authfile()

        user = authorizer.authenticate(username="admin", password="secret")

        assert user is not None
        assert user.username == "admin"


class TestGetAuthorizer:
    def test_returns_http_authorizer_when_authfile_configured(self) -> None:
        config = Config(registry_authfile="/tmp/auth.json")
        http_authorizer = MagicMock(spec=HTTPAuthorizer)
        no_auth_authorizer = MagicMock(spec=NoAuthAndAuthz)

        assert get_authorizer(config, http_authorizer, no_auth_authorizer) is http_authorizer

    def test_returns_no_auth_authorizer_when_no_authfile_configured(self) -> None:
        config = Config(registry_authfile=None)
        http_authorizer = MagicMock(spec=HTTPAuthorizer)
        no_auth_authorizer = MagicMock(spec=NoAuthAndAuthz)

        assert get_authorizer(config, http_authorizer, no_auth_authorizer) is no_auth_authorizer


class _AwatchStub:
    """Replaces `watchfiles.awatch()` with a scripted, deterministic async generator.

    Each element of `batches_per_call` describes the behaviour of one `awatch(...)`
    invocation: either an iterable of change-batches to yield, or an exception
    instance to raise immediately (simulating cancellation or a watcher failure).
    """

    def __init__(self, batches_per_call: list) -> None:
        self._remaining = list(batches_per_call)
        self.call_count = 0

    def __call__(self, *_args, **_kwargs):
        self.call_count += 1
        if not self._remaining:
            raise AssertionError("awatch() called more times than the test scripted")
        return self._agen(self._remaining.pop(0))

    @staticmethod
    async def _agen(item):
        if isinstance(item, BaseException):
            raise item
        for batch in item:
            yield batch


class TestHTTPAuthorizerStart:
    async def test_reloads_and_stops_when_watch_is_cancelled(self, tmp_path) -> None:
        auth_file = _make_authfile(tmp_path)
        authorizer = HTTPAuthorizer(auth_file)
        stub = _AwatchStub([[{(Change.added, auth_file)}], asyncio.CancelledError()])

        with patch("karapace.core.auth.awatch", stub):
            await authorizer.start(stats=MagicMock(spec=StatsClient))
            await authorizer._refresh_auth_task

        assert stub.call_count == 2

    async def test_deleted_change_restarts_the_watch(self, tmp_path) -> None:
        """Verify that a Change.deleted event triggers an early break and watch restart.

        The stub yields TWO batches in the first awatch() call: a delete batch followed
        by a modify batch. If the `if Change.deleted in ...: break` logic works correctly,
        _load_authfile is called twice total: once during start() initialization, and once
        for the delete batch. The modify batch is never processed because the break exits
        the async-for loop early.

        Without the break, _load_authfile would be called THREE times (start + delete + modify).
        """
        auth_file = _make_authfile(tmp_path)
        authorizer = HTTPAuthorizer(auth_file)
        stub = _AwatchStub(
            [
                [
                    {(Change.deleted, auth_file)},
                    {(Change.modified, auth_file)},  # Should NOT be processed if break works
                ],
                asyncio.CancelledError(),
            ]
        )

        with patch("karapace.core.auth.awatch", stub), patch.object(authorizer, "_load_authfile") as mock_load:
            await authorizer.start(stats=MagicMock(spec=StatsClient))
            await authorizer._refresh_auth_task

        assert stub.call_count == 2
        # Critical assertion: 2 calls = start() init + delete batch.
        # If the break was missing, this would be 3 (start + delete + modify).
        assert mock_load.call_count == 2

    async def test_invalid_configuration_during_reload_is_logged_and_ignored(self, tmp_path) -> None:
        auth_file = _make_authfile(tmp_path)
        authorizer = HTTPAuthorizer(auth_file)
        stub = _AwatchStub([[{(Change.added, auth_file)}], asyncio.CancelledError()])

        with (
            patch("karapace.core.auth.awatch", stub),
            patch.object(authorizer, "_load_authfile", side_effect=[None, InvalidConfiguration("bad")]) as mock_load,
        ):
            await authorizer.start(stats=MagicMock(spec=StatsClient))
            await authorizer._refresh_auth_task

        assert mock_load.call_count == 2

    async def test_unexpected_exception_from_watch_is_reported_to_stats(self, tmp_path) -> None:
        auth_file = _make_authfile(tmp_path)
        authorizer = HTTPAuthorizer(auth_file)
        stub = _AwatchStub([RuntimeError("watcher blew up")])
        stats = MagicMock(spec=StatsClient)

        with patch("karapace.core.auth.awatch", stub):
            await authorizer.start(stats=stats)
            await authorizer._refresh_auth_task

        stats.unexpected_exception.assert_called_once()
        _, kwargs = stats.unexpected_exception.call_args
        assert kwargs["where"] == "schema_registry_authfile_reloader"


class TestHTTPAuthorizerClose:
    async def test_cancels_running_refresh_task(self, tmp_path) -> None:
        authorizer = HTTPAuthorizer(_make_authfile(tmp_path))
        never_ending = asyncio.get_event_loop().create_task(asyncio.Event().wait())
        authorizer._refresh_auth_task = never_ending

        await authorizer.close()

        assert authorizer._refresh_auth_task is None
        assert authorizer._refresh_auth_awatch_stop_event.is_set()
        with pytest.raises(asyncio.CancelledError):
            await never_ending

    async def test_close_without_running_task_is_a_noop(self, tmp_path) -> None:
        authorizer = HTTPAuthorizer(_make_authfile(tmp_path))
        await authorizer.close()
        assert authorizer._refresh_auth_task is None


class TestMain:
    def test_prints_hash_json_with_provided_username_and_salt(self, monkeypatch, capsys) -> None:
        monkeypatch.setattr(sys, "argv", ["karapace_mkpasswd", "-u", "admin", "-a", "sha256", "my-password", "my-salt"])

        exit_code = main()

        assert exit_code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["username"] == "admin"
        assert parsed["algorithm"] == "sha256"
        assert parsed["salt"] == "my-salt"
        assert parsed["password_hash"] == hash_password(HashAlgorithm.SHA256, "my-salt", "my-password")

    def test_omits_username_and_generates_salt_when_not_provided(self, monkeypatch, capsys) -> None:
        monkeypatch.setattr(sys, "argv", ["karapace_mkpasswd", "my-password"])

        exit_code = main()

        assert exit_code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert "username" not in parsed
        assert parsed["algorithm"] == "sha512"
        assert isinstance(parsed["salt"], str) and len(parsed["salt"]) > 0
