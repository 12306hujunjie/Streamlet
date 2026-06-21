"""Tests for @node decorator with various parameter combinations."""

import pickle
import warnings
from typing import Annotated

import pytest
from dependency_injector.wiring import Provide

from streamlet import BaseFlowContext, Node, node


async def _resolve_value(value: int) -> int:
    return value


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
    def test_sync_node_returning_coroutine_is_rejected_from_sync_entrypoints(self):
        @node
        def func(x: int):
            return _resolve_value(x)

        with pytest.raises(TypeError, match="sync node 'func' returned an awaitable"):
            func(5)
        with pytest.raises(TypeError, match="sync node 'func' returned an awaitable"):
            func._execute(5)

    @pytest.mark.asyncio
    async def test_sync_node_returning_coroutine_is_rejected_in_event_loop(self):
        @node
        def func(x: int):
            return _resolve_value(x)

        with pytest.raises(TypeError, match="sync node 'func' returned an awaitable"):
            func._execute(5)
        with pytest.raises(TypeError, match="sync node 'func' returned an awaitable"):
            await func._execute_async(5)

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

    @pytest.mark.asyncio
    async def test_async_node_propagates_sync_runtime_error_once_in_event_loop(self):
        attempts = 0

        def fail_sync() -> None:
            nonlocal attempts
            attempts += 1
            raise RuntimeError("user failure")

        func = Node(fail_sync, name="fail_sync", is_async=True)

        with pytest.raises(RuntimeError, match="user failure"):
            func()

        assert attempts == 1


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
        from streamlet import ValidationInputException

        @node
        def double(x: int) -> int:
            return x * 2

        with pytest.raises(ValidationInputException):
            double("not_int")

    def test_invalid_return_type_raises(self):
        from streamlet import ValidationOutputException

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


class TestNodeFluentValidation:
    """Node fluent API should reject invalid user arguments at the boundary."""

    @pytest.fixture
    def source(self):
        @node
        def source_node(value: int) -> int:
            return value

        return source_node

    def test_then_rejects_non_node(self, source):
        with pytest.raises(TypeError, match="other must be a Node"):
            source.then(object())

    def test_fan_in_rejects_non_node(self, source):
        with pytest.raises(TypeError, match="aggregator must be a Node"):
            source.fan_in(object())

    def test_fan_out_to_rejects_non_string_executor(self, source):
        with pytest.raises(TypeError, match="executor must be a string"):
            source.fan_out_to([source], executor=object())

    def test_fan_out_to_rejects_non_list_targets(self, source):
        with pytest.raises(TypeError, match="nodes must be a list"):
            source.fan_out_to(object())

    def test_fan_out_to_rejects_non_node_targets(self, source):
        with pytest.raises(TypeError, match=r"nodes\[0\] must be a Node"):
            source.fan_out_to([object()])

    @pytest.mark.parametrize("max_workers", [True, 1.5])
    def test_fan_out_to_rejects_non_integer_max_workers(self, source, max_workers):
        with pytest.raises(TypeError, match="max_workers must be an int or None"):
            source.fan_out_to([source], max_workers=max_workers)

    @pytest.mark.parametrize("max_workers", [0, -1])
    def test_fan_out_to_rejects_non_positive_max_workers(self, source, max_workers):
        with pytest.raises(ValueError, match="max_workers must be greater than 0"):
            source.fan_out_to([source], max_workers=max_workers)

    def test_branch_on_rejects_non_dict_conditions(self, source):
        with pytest.raises(TypeError, match="conditions must be a dict"):
            source.branch_on(object())

    def test_branch_on_rejects_non_node_branch(self, source):
        with pytest.raises(TypeError, match=r"conditions\[1\] must be a Node"):
            source.branch_on({1: object()})
