"""pytest configuration for AetherFlow tests."""

import pytest

from src.aetherflow import BaseFlowContext


@pytest.fixture
def container():
    """Function-scoped dependency injection container."""
    return BaseFlowContext()
