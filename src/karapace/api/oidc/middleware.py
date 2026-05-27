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
        self.require_at_jwt_typ = config.sasl_oauthbearer_require_at_jwt_typ
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
                    "OIDC: JWKS URL uses plain HTTP (%s) — INSECURE override is active. " "DO NOT use in production.",
                    self.jwks_url,
                )
            if not self.issuer or not self.audience:
                raise ValueError(
                    "OIDC config error: 'issuer' and 'audience' must be set if 'jwks_endpoint_url' is provided."
                )
            if self.enforce_azp and not self.client_id:
                raise ValueError("OIDC config error: client_id is required when sasl_oauthbearer_enforce_azp is enabled.")
            log.info(
                "OIDC middleware initialized — Bearer token validation enabled. " "jwks_url=%s issuer=%s audience=%s",
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

                required_http_methods = set(self.sasl_oauthbearer_method_roles.keys())
                # Validate required HTTP methods in method_roles
                missing_methods = required_http_methods - self.sasl_oauthbearer_method_roles.keys()
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
        # Defense-in-depth: __init__ enforces audience when JWKS is set, but a misconfigured
        # validator should fail closed rather than disable aud checks via audience=None.
        if not self.audience:
            raise AuthenticationError("OIDC not configured: audience missing")

        try:
            if self.require_at_jwt_typ:
                # RFC 9068: access tokens carry `typ: at+jwt`. Reject ID tokens or other
                # JWT shapes being misused as access tokens. Header is unverified bytes,
                # but it's only used to gate; the signature is still checked below.
                header_typ = jwt.get_unverified_header(token).get("typ", "")
                if header_typ.lower() != "at+jwt":
                    raise InvalidTokenError(f"Invalid token type: expected at+jwt, got {header_typ!r}")

            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
            audiences = [aud.strip() for aud in self.audience.split(",") if aud.strip()]
            require = ["exp", "iss", "aud"]
            if self.claim_name:
                require.append(self.claim_name)
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=self.algorithms,
                audience=audiences,
                issuer=self.issuer,
                leeway=self.leeway_seconds,
                # Require exp/iss/aud (and the configured subject claim) so a token missing
                # any of these is rejected — PyJWT does not require them by default.
                options={"require": require},
            )
            if self.enforce_azp and payload.get("azp") != self.client_id:
                raise InvalidTokenError("azp claim does not match configured client_id")
            return payload
        except ExpiredSignatureError:
            # Surface expiry distinctly so callers can return a clearer 401 reason
            # and operators can debug clock-skew vs. real-auth failures.
            log.warning("JWT validation failed: token expired")
            raise TokenExpiredError("OIDC token expired")
        except InvalidTokenError as exc:
            # Log the underlying reason for operator debugging; keep the response opaque.
            log.warning("JWT validation failed: %s", exc)
            raise AuthenticationError("Invalid OIDC token")

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
