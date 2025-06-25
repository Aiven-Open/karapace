from src.karapace.api.oidc.validator import OIDCValidator
from src.karapace.core.auth import AuthenticationError
from src.karapace.core.config import Config


class OIDCMiddleware:
    def __init__(self, app, config: Config):
        self.app = app
        self.config = config
        self.validator = OIDCValidator(config)

    async def __call__(self, scope, receive, send):
        headers = dict(scope["headers"])
        auth_header = headers.get(b"authorization", b"").decode()

        if auth_header.startswith("Bearer "):
            try:
                payload = self.validator.validate_jwt(auth_header.split(" ", 1)[1])
                scope["user"] = payload  # Or inject into request
            except AuthenticationError as e:
                from starlette.responses import JSONResponse
                await JSONResponse({"error": "Unauthorized", "reason": str(e)}, status_code=401)(scope, receive, send)
                return

        await self.app(scope, receive, send)
