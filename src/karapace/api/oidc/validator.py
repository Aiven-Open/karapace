import jwt
import logging
from jwt import PyJWKClient, InvalidTokenError

from src.karapace.core.auth import AuthenticationError
from src.karapace.core.config import Config

log = logging.getLogger(__name__)


class OIDCValidator:
    def __init__(self, config: Config):
        self.config = config
        self.jwks_url = config.oauthbearer_jwks_endpoint_url
        self.issuer = config.oauthbearer_expected_issuer
        self.audience = config.oauthbearer_expected_audience
        self._jwks_client = PyJWKClient(self.jwks_url)

    def validate_jwt(self, token: str) -> dict:
        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256", "RS384", "RS512"],
                audience=self.audience,
                issuer=self.issuer,
            )
            return payload
        except InvalidTokenError as e:
            log.error(f"JWT validation failed: {e}")
            raise AuthenticationError("Invalid OIDC token")


def authenticate_oidc_request(auth_header: str, config: Config) -> dict:
    """Authenticate a request using OIDC Bearer token and config values."""
    if not auth_header or not auth_header.startswith("Bearer "):
        raise AuthenticationError("Missing or invalid Authorization header")
    token = auth_header.split(" ", 1)[1]
    validator = OIDCValidator(config)
    return validator.validate_jwt(token)


def enable_oidc_auth(app, config):
    """Add OIDC authentication middleware to the app."""
    try:
        from karapace.core.oidc_karapace import OIDCMiddleware
    except ImportError:
        raise ImportError("OIDCMiddleware not found. Please ensure oidc_karapace.py exists and is correct.")
    app.add_middleware(OIDCMiddleware, config=config)
