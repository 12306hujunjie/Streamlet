"""Tests for Node.then() sequential composition."""

import pytest

from src.streamlet import node
from tests.conftest import add_ten, async_add_ten, async_double, double, to_string


class TestThenBasic:
    def test_two_node_chain(self):
        flow = double.then(add_ten)
        result = flow(5)
        assert result == 20  # (5 * 2) + 10

    def test_three_node_chain(self):
        flow = double.then(add_ten).then(to_string)
        result = flow(5)
        assert result == "result:20"

    def test_chain_preserves_data_flow(self):
        flow = add_ten.then(double)
        result = flow(5)
        assert result == 30  # (5 + 10) * 2

    def test_chain_returns_node(self):
        from src.streamlet import Node

        flow = double.then(add_ten)
        assert isinstance(flow, Node)


class TestThenAsyncSyncMixing:
    @pytest.mark.asyncio
    async def test_sync_then_async(self):
        flow = double.then(async_add_ten)
        result = await flow(5)
        assert result == 20

    @pytest.mark.asyncio
    async def test_async_then_sync(self):
        flow = async_double.then(add_ten)
        result = await flow(5)
        assert result == 20

    @pytest.mark.asyncio
    async def test_async_then_async(self):
        flow = async_double.then(async_add_ten)
        result = await flow(5)
        assert result == 20

    @pytest.mark.asyncio
    async def test_mixed_multi_level(self):
        flow = double.then(async_add_ten).then(to_string)
        result = await flow(5)
        assert result == "result:20"


class TestThenErrorPropagation:
    def test_error_in_first_node_propagates(self):
        @node
        def failing_node(x: int) -> int:
            raise ValueError("first node failed")

        flow = failing_node.then(double)
        with pytest.raises(ValueError, match="first node failed"):
            flow(5)

    def test_error_in_second_node_propagates(self):
        @node
        def failing_node(x: int) -> int:
            raise RuntimeError("second node failed")

        flow = double.then(failing_node)
        with pytest.raises(RuntimeError, match="second node failed"):
            flow(5)

    @pytest.mark.asyncio
    async def test_error_in_async_chain_propagates(self):
        @node
        async def async_failing(x: int) -> int:
            raise ValueError("async failed")

        flow = double.then(async_failing)
        with pytest.raises(ValueError, match="async failed"):
            await flow(5)
