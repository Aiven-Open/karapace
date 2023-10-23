"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from aiokafka.abc import AbstractTokenProvider as AbstractTokenProviderAsync
from http import HTTPStatus
from kafka.oauth.abstract import AbstractTokenProvider
from karapace.config import Config
from karapace.rapu import HTTPResponse, JSON_CONTENT_TYPE
from typing import NoReturn, TypedDict

import aiohttp
import dataclasses
import datetime
import enum
import jwt


@enum.unique
class TokenType(enum.Enum):
    BASIC = "Basic"
    BEARER = "Bearer"


def raise_unauthorized() -> NoReturn:
    raise HTTPResponse(
        body='{"message": "Unauthorized"}',
        status=HTTPStatus.UNAUTHORIZED,
        content_type=JSON_CONTENT_TYPE,
        headers={"WWW-Authenticate": 'Basic realm="Karapace REST Proxy"'},
    )


class SASLPlainConfig(TypedDict):
    sasl_mechanism: str | None
    sasl_plain_username: str | None
    sasl_plain_password: str | None


class SASLOauthConfig(TypedDict):
    sasl_mechanism: str | None
    sasl_oauth_token: str | None


def _split_auth_header(auth_header: str) -> tuple[str, str]:
    token_type, _separator, token = auth_header.partition(" ")
    return (token_type, token)


def get_auth_config_from_header(
    auth_header: str | None,
    config: Config,
) -> SASLPlainConfig | SASLOauthConfig:
    """Verify the given Authorization HTTP header and constructs config parameters based on it.

    In case the Authorization header is `None`, or unknown, raises an Unauthorized HTTP response.
    Known/possible authentication tokens are `Bearer` and `Basic`.

    :param auth_header: The Authorization header extracted from an HTTP request
    :param config: Current config of Karapace, necessary to decide on the SASL mechanism
    """
    if auth_header is None:
        raise_unauthorized()

    token_type, token = _split_auth_header(auth_header)

    if token_type == TokenType.BEARER.value:
        return {"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": token}

    if token_type == TokenType.BASIC.value:
        basic_auth = aiohttp.BasicAuth.decode(auth_header)
        sasl_mechanism = config["sasl_mechanism"]
        if sasl_mechanism is None:
            sasl_mechanism = "PLAIN"

        return {
            "sasl_mechanism": sasl_mechanism,
            "sasl_plain_username": basic_auth.login,
            "sasl_plain_password": basic_auth.password,
        }

    raise_unauthorized()


def get_expiration_time_from_header(auth_header: str) -> datetime.datetime | None:
    """Extract expiration from Authorization HTTP header.

    In case of an OAuth Bearer token, the `exp` claim is extracted and returned as a
    `datetime.datetime` object. Otherwise it's safely assumed that the authentication
    method is Basic, thus no expiry of the credentials.

    The signature is not verified as it is done by the Kafka clients using it and
    discarding the token in case of any issues.

    :param auth_header: The Authorization header extracted from an HTTP request
    """
    token_type, token = _split_auth_header(auth_header)

    if token_type == TokenType.BEARER.value:
        try:
            exp_claim = jwt.decode(token, options={"verify_signature": False}).get("exp")
        except jwt.exceptions.DecodeError:
            raise_unauthorized()

        if exp_claim is not None:
            return datetime.datetime.fromtimestamp(exp_claim, datetime.timezone.utc)

    return None


@dataclasses.dataclass
class SimpleOauthTokenProvider(AbstractTokenProvider):
    """A pass-through OAuth token provider to be used by synchronous Kafka clients.

    The token is meant to be extracted from an HTTP Authorization header.
    """

    _token: str

    def token(self) -> str:
        return self._token


@dataclasses.dataclass
class SimpleOauthTokenProviderAsync(AbstractTokenProviderAsync):
    """A pass-through OAuth token provider to be used by asynchronous Kafka clients.

    The token is meant to be extracted from an HTTP Authorization header.
    """

    _token: str

    async def token(self) -> str:
        return self._token


class SASLOauthParams(TypedDict):
    sasl_mechanism: str
    sasl_oauth_token_provider: AbstractTokenProvider | AbstractTokenProviderAsync


def get_kafka_client_auth_parameters_from_config(
    config: Config,
    *,
    async_client: bool = True,
) -> SASLPlainConfig | SASLOauthParams:
    """Create authentication parameters for a Kafka client based on the Karapace config.

    In case of an `OAUTHBEARER` SASL mechanism present in the config, will create the
    OAuth token provider needed by the Kafka client - the `async_client` parameter
    decides whether this will be a sync or async one.

    :param config: Current config of Karapace
    :param async_client: Flag to indicate whether the Kafka client using the returned paramaters is async
    """
    if config["sasl_mechanism"] == "OAUTHBEARER":
        token_provider_cls = SimpleOauthTokenProviderAsync if async_client else SimpleOauthTokenProvider
        return {
            "sasl_mechanism": config["sasl_mechanism"],
            "sasl_oauth_token_provider": token_provider_cls(config["sasl_oauth_token"]),
        }

    return {
        "sasl_mechanism": config["sasl_mechanism"],
        "sasl_plain_username": config["sasl_plain_username"],
        "sasl_plain_password": config["sasl_plain_password"],
    }
