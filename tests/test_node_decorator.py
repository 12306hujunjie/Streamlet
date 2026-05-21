"""Tests for @node decorator with various parameter combinations."""

import pytest
from dependency_injector.wiring import Provide

from src.streamlet import BaseFlowContext, Node, node


class TestNodeDecoratorCallModes:
    def test_node_without_parentheses(self):
        @node
        def func(x: int) -> int:
            return x * 2

        assert isinstance(func, Node)
        assert func(5) == 10

    def test_node_with_parentheses_no_args(self):
        @node()
        def func(x: int) -> int:
            return x * 2

        assert isinstance(func, Node)
        assert func(5) == 10

    def test_node_with_explicit_name(self):
        @node(name="custom_name")
        def func(x: int) -> int:
            return x * 2

        assert func.name == "custom_name"


class TestNodeDecoratorAsync:
    @pytest.mark.asyncio
    async def test_async_node_without_parentheses(self):
        @node
        async def func(x: int) -> int:
            return x * 2

        assert func.is_async is True
        assert await func(5) == 10

    @pytest.mark.asyncio
    async def test_async_node_with_name(self):
        @node(name="async_node")
        async def func(x: int) -> int:
            return x * 2

        assert func.name == "async_node"
        assert func.is_async is True


class TestNodeDecoratorWithDI:
    def test_node_with_dependency_injection(self):
        container = BaseFlowContext()
        container.state()["key"] = "di_value"

        @node
        def di_node(x: int, state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"x": x, "state_key": state.get("key")}

        container.wire(modules=[__name__])

        result = di_node(42)
        assert result["x"] == 42
        assert result["state_key"] == "di_value"


class TestNodeDecoratorTypeValidation:
    def test_valid_type_passes(self):
        @node
        def double(x: int) -> int:
            return x * 2

        assert double(5) == 10

    def test_invalid_type_raises(self):
        from src.streamlet import ValidationInputException

        @node
        def double(x: int) -> int:
            return x * 2

        with pytest.raises(ValidationInputException):
            double("not_int")

    def test_invalid_return_type_raises(self):
        from src.streamlet import ValidationOutputException

        @node
        def bad_return(x: int) -> str:
            return 42  # returns int, annotated as str

        with pytest.raises(ValidationOutputException):
            bad_return(5)


class TestNodeProperties:
    """Node 实例的底层属性与编码行为。"""

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

    def test_node_wraps_async_node(self):
        @node
        async def async_func(x: int) -> int:
            return x * 2

        wrapper = Node(func=async_func, name="wrapper")
        assert wrapper.is_async is True

    def test_explicit_is_async_override(self):
        @node
        async def async_func(x: int) -> int:
            return x * 2

        wrapper = Node(func=async_func.func, name="wrapper", is_async=True)
        assert wrapper.is_async is True
