"""Tests for Node class basics: creation, properties, is_async detection."""

import pytest

from src.aetherflow import Node, node


class TestNodeCreation:
    def test_node_name_from_function(self):
        @node
        def my_func(x: int) -> int:
            return x * 2

        assert isinstance(my_func, Node)
        assert my_func.name == "my_func"

    def test_node_explicit_name(self):
        @node(name="explicit_name")
        def my_func(x: int) -> int:
            return x * 2

        assert my_func.name == "explicit_name"

    def test_node_func_access(self):
        @node
        def my_func(x: int) -> int:
            return x * 2

        assert callable(my_func.func)
        assert my_func.func(5) == 10

    def test_node_repr(self):
        @node
        def my_func(x: int) -> int:
            return x * 2

        assert repr(my_func) == "Node(name='my_func')"


class TestNodeIsAsync:
    def test_sync_node_is_async_false(self):
        @node
        def sync_func(x: int) -> int:
            return x * 2

        assert sync_func.is_async is False

    def test_async_node_is_async_true(self):
        @node
        async def async_func(x: int) -> int:
            return x * 2

        assert async_func.is_async is True

    def test_node_wrapping_async_node(self):
        @node
        async def async_func(x: int) -> int:
            return x * 2

        wrapper = Node(func=async_func, name="wrapper")
        # When func is a Node, is_async should come from the wrapped node
        assert wrapper.is_async is True

    def test_explicit_is_async_override(self):
        @node
        async def async_func(x: int) -> int:
            return x * 2

        wrapper = Node(func=async_func.func, name="wrapper", is_async=True)
        assert wrapper.is_async is True


class TestNodeCall:
    def test_sync_node_direct_call(self):
        @node
        def double(x: int) -> int:
            return x * 2

        result = double(5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_async_node_await(self):
        @node
        async def double(x: int) -> int:
            return x * 2

        result = await double(5)
        assert result == 10

    def test_sync_node_in_async_context(self):
        @node
        def double(x: int) -> int:
            return x * 2

        result = double(5)
        assert result == 10
