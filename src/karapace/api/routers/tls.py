"""Internal endpoints related to TLS configuration.

The endpoints in this module are intended for operational use from
trusted networks. They allow validating and reloading the TLS server
configuration without restarting the process.

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from datetime import datetime

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Header, HTTPException, status
from karapace.api.container import SchemaRegistryContainer
from karapace.core.config import Config
from karapace.core.tls import TlsContextHolder, TlsReloadResult
from pydantic import BaseModel, Field


tls_router = APIRouter(
    prefix="/internal/tls",
    tags=["tls"],
)


class TLSReloadResponse(BaseModel):
    status: str
    message: str | None = None
    mode: str = Field(default="prepare_only", description="Current behaviour of the reload endpoint")
    reloaded_at: datetime | None = None


def _validate_token(
    *,
    provided_token: str | None,
    expected_token: str | None,
) -> None:
    if expected_token is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TLS reload token is not configured",
        )

    if not provided_token or provided_token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid TLS reload token",
        )


@tls_router.post("/reload", response_model=TLSReloadResponse)
@inject
async def reload_tls(
    x_tls_reload_token: str | None = Header(default=None, alias="X-TLS-Reload-Token"),
    config: Config = Depends(Provide[SchemaRegistryContainer.karapace_container.config]),
    tls_context_holder: TlsContextHolder = Depends(Provide[SchemaRegistryContainer.tls_context_holder]),
) -> TLSReloadResponse:
    """Reload server TLS context from configuration.

    The endpoint validates current TLS-related configuration and
    certificate files by attempting to rebuild the server `SSLContext`.

    In the current implementation (mode == "prepare_only") the runtime
    HTTP listener still uses the TLS configuration created at process
    start; the new context is stored in `TlsContextHolder` and will be
    used only on the next restart or if additional wiring is added in
    the future.
    """

    _validate_token(provided_token=x_tls_reload_token, expected_token=config.server_tls_reload_token)

    result: TlsReloadResult = await tls_context_holder.reload()

    if not result.success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=result.message or "Failed to reload TLS context",
        )

    return TLSReloadResponse(
        status="ok",
        message=result.message,
        mode="prepare_only",
        reloaded_at=result.reloaded_at,
    )
