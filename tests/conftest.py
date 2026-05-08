"""pytest configuration for Streamlet tests."""

import pytest

from src.streamlet import BaseFlowContext


@pytest.fixture
def container():
    """Function-scoped dependency injection container."""
    return BaseFlowContext()
