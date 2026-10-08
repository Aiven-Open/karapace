"""
Tests for examples/msk_iam_token_provider.py.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

import copy
import importlib.util
import sys
import types
from pathlib import Path

import pytest

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "msk_iam_token_provider.py"


@pytest.fixture
def example_module():
    spec = importlib.util.spec_from_file_location("msk_iam_token_provider_example", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fake_signer(monkeypatch):
    """Stand in for aws_msk_iam_sasl_signer, which is not a test dependency."""
    calls: list[str] = []

    class MSKAuthTokenProvider:
        @staticmethod
        def generate_auth_token(region):
            calls.append(region)
            return "msk-token", 1_800_000_000_000  # the signer reports milliseconds

    fake = types.ModuleType("aws_msk_iam_sasl_signer")
    fake.MSKAuthTokenProvider = MSKAuthTokenProvider
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", fake)
    return calls


def test_instance_is_deepcopyable(example_module):
    # Config stores the instance and dependency_injector deepcopies Config
    # while wiring containers; a module cached on self breaks that with
    # "TypeError: cannot pickle 'module' object".
    provider = example_module.MSKIAMTokenProvider(region="eu-west-1")
    clone = copy.deepcopy(provider)
    assert clone is not provider
    assert clone._get_region() == "eu-west-1"


def test_constructing_does_not_require_the_aws_signer(example_module, monkeypatch):
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", None)  # import would raise
    example_module.MSKIAMTokenProvider()


def test_token_with_expiry_returns_epoch_seconds(example_module, fake_signer):
    provider = example_module.MSKIAMTokenProvider(region="us-west-2")
    token, expiry = provider.token_with_expiry()
    assert token == "msk-token"
    assert expiry == 1_800_000_000  # seconds, as librdkafka's oauth_cb expects
    assert fake_signer == ["us-west-2"]


def test_region_falls_back_to_env(example_module, fake_signer, monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-2")
    provider = example_module.MSKIAMTokenProvider()
    provider.token_with_expiry()
    assert fake_signer == ["ap-southeast-2"]
