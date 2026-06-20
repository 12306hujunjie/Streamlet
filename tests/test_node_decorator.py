"""Tests for @node decorator with various parameter combinations."""

import pickle
import warnings
from typing import Annotated

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

    def test_plain_node_does_not_emit_di_wiring_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            @node
            def func(x: int) -> int:
                return x

        messages = [str(warning.message) for warning in caught]
        assert not any("@inject is not required here" in msg for msg in messages)
        assert func(5) == 5


class TestNodeDecoratorAsync:
    @pytest.mark.asyncio
    async def test_async_node_without_parentheses(self):
        @node
        async def func(x: int) -> int:
            return x * 2

        assert await func(5) == 10

    @pytest.mark.asyncio
    async def test_async_node_with_name(self):
        @node(name="async_node")
        async def func(x: int) -> int:
            return x * 2

        assert func.name == "async_node"
        assert await func(5) == 10


class TestNodeDecoratorWithDI:
    def test_node_with_dependency_injection(self):
        container = BaseFlowContext()
        container.context()["key"] = "di_value"

        @node
        def di_node(x: int, state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"x": x, "state_key": state.get("key")}

        container.wire(modules=[__name__])

        result = di_node(42)
        assert result["x"] == 42
        assert result["state_key"] == "di_value"

    def test_node_with_annotated_dependency_injection(self):
        container = BaseFlowContext()
        container.context()["key"] = "annotated_value"

        @node
        def di_node(
            state: Annotated[dict, Provide[BaseFlowContext.context]],
        ) -> dict:
            return {"state_key": state.get("key")}

        container.wire(modules=[__name__])

        result = di_node()
        assert result["state_key"] == "annotated_value"


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

    def test_custom_return_type_passes(self):
        class PlainResult:
            def __init__(self, value: int) -> None:
                self.value = value

        @node
        def create_result(x: int) -> PlainResult:
            return PlainResult(x)

        result = create_result(5)
        assert isinstance(result, PlainResult)
        assert result.value == 5


class TestNodeProperties:
    """Node 实例的基础属性。"""

    def test_node_repr(self):
        @node
        def my_func(x: int) -> int:
            return x * 2

        assert "my_func" in repr(my_func)

    def test_node_rejects_pickle_serialization(self):
        @node
        def my_func(x: int) -> int:
            return x * 2

        with pytest.raises(TypeError, match="not pickle-serializable"):
            pickle.dumps(my_func)
