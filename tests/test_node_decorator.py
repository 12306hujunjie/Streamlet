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


class TestNodeDecoratorRetry:
    def test_node_with_retry_enabled(self):
        call_count = 0

        class TempError(Exception):
            retryable = True

        @node(
            retry_count=2,
            retry_delay=0.01,
            exception_types=(TempError,),
            enable_retry=True,
        )
        def flaky(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TempError("retry")
            return x * 2

        result = flaky(5)
        assert result == 10
        assert call_count == 3

    def test_node_retry_disabled_by_default(self):
        @node
        def func(x: int) -> int:
            return x * 2

        assert func(5) == 10

    def test_node_custom_retry_config(self):
        @node(
            retry_count=5,
            retry_delay=0.5,
            backoff_factor=2.0,
            max_delay=30.0,
            enable_retry=True,
        )
        def func(x: int) -> int:
            return x * 2

        assert func(5) == 10


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
