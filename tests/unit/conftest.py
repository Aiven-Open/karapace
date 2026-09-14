"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import pytest

from karapace.core.serialization import sr_authorization_ctx


@pytest.fixture
def reset_sr_authorization_ctx():
    """Reset the contextvar between tests."""
    token = sr_authorization_ctx.set(None)
    try:
        yield
    finally:
        sr_authorization_ctx.reset(token)
