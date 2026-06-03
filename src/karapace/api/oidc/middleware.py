"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from typing import Any

import jwt
import logging
from fastapi import FastAPI, HTTPException
from jwt import ExpiredSignatureError, PyJWKClient, InvalidTokenError
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config


class TokenExpiredError(AuthenticationError):
    """Raised when an OIDC JWT is rejected because its `exp` claim is in the past."""


log = logging.getLogger(__name__)

# Methods served by SR routers — each must appear in ``method_roles`` when authz
# is on, otherwise ``authorize_request`` silently 403s that method.
REQUIRED_HTTP_METHODS: frozenset[str] = frozenset({"GET", "POST", "PUT", "DELETE"})


class OIDCMiddleware:
    def __init__(self, app: FastAPI, config: Config) -> None:
        self.app = app
        self.config = config

        self._jwks_client: PyJWKClient | None
        self.jwks_url = config.sasl_oauthbearer_jwks_endpoint_url
        self.issuer = config.sasl_oauthbearer_expected_issuer
        self.audience = config.sasl_oauthbearer_expected_audience
        self.claim_name = config.sasl_oauthbearer_sub_claim_name
        self.authentication_enabled = config.sasl_oauthbearer_authentication_enabled
        self.authorization_enabled = config.sasl_oauthbearer_authorization_enabled
        self.client_id = config.sasl_oauthbearer_client_id
        self.sasl_oauthbearer_method_roles: dict[str, list[str]] = config.sasl_oauthbearer_method_roles
        self.sasl_oauthbearer_roles_claim_path = config.sasl_oauthbearer_roles_claim_path
        self.leeway_seconds = config.sasl_oauthbearer_leeway_seconds
        self.require_access_token_typ = config.sasl_oauthbearer_require_access_token_typ
        self.enforce_azp = config.sasl_oauthbearer_enforce_azp

        # Hardcoded default algorithms
        self.algorithms = ["RS256", "RS384", "RS512"]

        # Validate required fields if JWKS URL is set
        if self.jwks_url:
            if not self.jwks_url.lower().startswith("https://"):
                if not config.sasl_oauthbearer_allow_insecure_jwks:
                    # Plain HTTP lets an in-path attacker swap the signing keys and
                    # forge tokens that pass validation. Fail closed by default.
                    raise ValueError(
                        "OIDC config error: sasl_oauthbearer_jwks_endpoint_url must use https://. "
                        "Set sasl_oauthbearer_allow_insecure_jwks=true to override (dev only)."
                    )
                log.warning(
                    "OIDC: JWKS URL uses plain HTTP (%s) — INSECURE override is active. DO NOT use in production.",
                    self.jwks_url,
                )
            if not self.issuer or not self.audience:
                raise ValueError(
                    "OIDC config error: 'issuer' and 'audience' must be set if 'jwks_endpoint_url' is provided."
                )
            if self.enforce_azp and not self.client_id:
                raise ValueError("OIDC config error: client_id is required when sasl_oauthbearer_enforce_azp is enabled.")
            log.info(
                "OIDC middleware initialized — Bearer token validation enabled. jwks_url=%s issuer=%s audience=%s",
                self.jwks_url,
                self.issuer,
                self.audience,
            )

            # lifespan caps how long a key stays cached after IdP rotation/revocation.
            # max_cached_keys=16 is generous; IdPs typically expose 1-3 active signing keys.
            self._jwks_client = PyJWKClient(self.jwks_url, cache_keys=True, lifespan=300, max_cached_keys=16)

            log.info("OIDC Authorization enabled: %s", self.authorization_enabled)
            if self.authorization_enabled:
                if self.client_id is None or self.sasl_oauthbearer_roles_claim_path is None:
                    raise ValueError(
                        "OIDC config error: client_id and roles_claim_path are required when authorization is enabled."
                    )

                # Fail fast on incomplete ``method_roles`` rather than silently 403 at request time.
                configured_methods = {m.upper() for m in self.sasl_oauthbearer_method_roles.keys()}
                missing_methods = REQUIRED_HTTP_METHODS - configured_methods
                if missing_methods:
                    raise ValueError(
                        f"OIDC config error: method_roles is missing definitions for: {', '.join(sorted(missing_methods))}"
                    )
                log.info(
                    "OIDC Authorization configured. — client_id: %s, method_roles: %s, roles_claim_path: %s",
                    self.client_id,
                    self.sasl_oauthbearer_method_roles,
                    self.sasl_oauthbearer_roles_claim_path,
                )
        else:
            if self.authentication_enabled or self.authorization_enabled:
                raise ValueError(
                    "OIDC config error: sasl_oauthbearer_jwks_endpoint_url is required when "
                    "authentication or authorization is enabled. Also set expected_issuer and expected_audience."
                )
            self._jwks_client = None

    def validate_jwt(self, token: str) -> dict:
        if not self._jwks_client:
            raise AuthenticationError("OIDC not configured: JWKS client unavailable")
        # Fail closed if audience is somehow unset at decode time; __init__ already enforces it.
        if not self.audience:
            raise AuthenticationError("OIDC not configured: audience missing")

        try:
            self._check_at_jwt_typ(token)
            payload = self._decode_and_verify(token)
            self._check_azp(payload)
            return payload
        except ExpiredSignatureError:
            log.warning("JWT validation failed: token expired")
            raise TokenExpiredError("OIDC token expired")
        except InvalidTokenError as exc:
            # Log the reason for debugging; response body stays opaque.
            log.warning("JWT validation failed: %s", exc)
            raise AuthenticationError("Invalid OIDC token")

    def _check_at_jwt_typ(self, token: str) -> None:
        if not self.require_access_token_typ:
            return
        # Header is unverified, but only gates rejection; signature is still checked at decode.
        header_typ = jwt.get_unverified_header(token).get("typ", "")
        if header_typ.lower() not in ("at+jwt", "application/at+jwt"):
            raise InvalidTokenError(f"Invalid token type: expected at+jwt, got {header_typ!r}")

    def _decode_and_verify(self, token: str) -> dict:
        assert self._jwks_client is not None and self.audience  # validated by validate_jwt
        signing_key = self._jwks_client.get_signing_key_from_jwt(token)
        audiences = {aud.strip() for aud in self.audience.split(",") if aud.strip()}
        require = {"exp", "iss", "aud"}
        if self.claim_name:
            require.add(self.claim_name)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=self.algorithms,
            audience=audiences,
            issuer=self.issuer,
            leeway=self.leeway_seconds,
            # PyJWT does not require these by default; enforce presence explicitly.
            options={"require": list(require)},
        )

    def _check_azp(self, payload: dict) -> None:
        if not self.enforce_azp:
            return
        if payload.get("azp") != self.client_id:
            raise InvalidTokenError("azp claim does not match configured client_id")

    def authorize_request(self, payload: dict, request_method: str) -> bool:
        if not self.authorization_enabled:
            return True

        request_method = request_method.upper()
        allowed_roles = self.sasl_oauthbearer_method_roles.get(request_method, [])

        # Dynamic role extraction based on configured path
        if self.sasl_oauthbearer_roles_claim_path is not None and self.client_id is not None:
            roles_claim_path = self.sasl_oauthbearer_roles_claim_path.replace("[client_id]", self.client_id)
        else:
            # Same body as the role-mismatch branch below so an attacker cannot use the
            # response to distinguish a valid token with bad config from a token that
            # simply lacks the required role.
            log.error("Authorization misconfigured: roles_claim_path or client_id is unset")
            raise HTTPException(status_code=403, detail="Forbidden")

        user_roles = self.get_roles_from_claim_path(payload, roles_claim_path)

        if not any(role in user_roles for role in allowed_roles):
            log.warning(
                "Authorization failed for method %s. User roles: %s, required: %s",
                request_method,
                user_roles,
                allowed_roles,
            )
            raise HTTPException(status_code=403, detail="Forbidden")
        log.debug("Authorized")
        return True

    @staticmethod
    def get_roles_from_claim_path(payload: dict[str, Any], path: str) -> list[str]:
        try:
            parts = path.split(".")
            value: Any = payload
            for part in parts:
                if not isinstance(value, dict):
                    return []  # path broken
                value = value.get(part)

            # value might not be a list of strings, so check
            if isinstance(value, list):
                if all(isinstance(role, str) for role in value):
                    return value
        except Exception:
            pass
        return []
