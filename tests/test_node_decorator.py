"""Tests for @node decorator with various parameter combinations."""

import asyncio
import inspect
import pickle
import sys
import time
import types
import warnings
from contextvars import ContextVar
from typing import Annotated

import pytest
from dependency_injector.wiring import Provide

from streamlet import (
    BaseFlowContext,
    ContextVarProvider,
    Node,
    NodeTimeoutException,
    ValidationInputException,
    node,
)


async def _resolve_value(value: int) -> int:
    return value


@node(name="strict_timeout_probe", timeout=1)
def _strict_timeout_probe() -> None:
    return None


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

    def test_node_with_direct_func_and_explicit_name(self):
        def func(x: int) -> int:
            return x * 2

        decorated = node(func, name="custom_name")

        assert isinstance(decorated, Node)
        assert decorated.name == "custom_name"
        assert decorated(5) == 10

    def test_node_with_direct_func_and_retry_options(self):
        call_count = 0

        class TempError(Exception):
            retryable = True

        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TempError("retry")
            return x * 2

        decorated = node(
            func,
            retry_count=1,
            retry_delay=0,
            exception_types=(TempError,),
            enable_retry=True,
        )

        assert decorated(5) == 10
        assert call_count == 2

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
    def test_sync_awaitable_is_closed_before_return_validation(self):
        pending = _resolve_value(5)

        @node
        def wrong() -> int:
            return pending

        try:
            with pytest.raises(TypeError, match="returned an awaitable"):
                wrong()
            assert inspect.getcoroutinestate(pending) == inspect.CORO_CLOSED
        finally:
            pending.close()

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


class TestNodeDecoratorTimeout:
    @pytest.mark.parametrize("async_node", [False, True])
    def test_business_timeout_is_not_reclassified(self, async_node):
        error = TimeoutError("upstream timeout")

        def sync_fail() -> None:
            raise error

        async def async_fail() -> None:
            raise error

        func = node(async_fail if async_node else sync_fail, timeout=1)
        with pytest.raises(TimeoutError) as exc_info:
            func()
        assert exc_info.value is error

    def test_sync_timeout_preserves_request_context_without_writeback(self):
        request_id = ContextVar("request_id", default="missing")
        token = request_id.set("request-1")

        @node(timeout=1)
        def read_request_id() -> str:
            value = request_id.get()
            request_id.set("worker")
            return value

        try:
            assert read_request_id() == "request-1"
            assert request_id.get() == "request-1"
        finally:
            request_id.reset(token)

    def test_sync_node_timeout_raises_and_stops_execution(self):
        events: list[str] = []

        @node(name="slow_sync", timeout=0.01)
        def slow_sync() -> str:
            events.append("started")
            time.sleep(0.05)
            events.append("finished")
            return "done"

        with pytest.raises(NodeTimeoutException) as exc_info:
            slow_sync()

        time.sleep(0.06)
        assert exc_info.value.node_name == "slow_sync"
        assert exc_info.value.timeout_seconds == 0.01
        assert events == ["started"]

    def test_sync_node_timeout_preserves_dependency_injection_context(self):
        container = BaseFlowContext()
        container.context()["key"] = "value"

        @node(name="sync_timeout_di", timeout=1)
        def sync_timeout_di(
            state: dict = Provide[BaseFlowContext.context],
        ) -> str:
            return state["key"]

        container.wire(modules=[__name__])

        assert sync_timeout_di() == "value"

    def test_sync_node_timeout_enforces_strict_context_validation(self):
        class StrictFlowContext(BaseFlowContext):
            context = ContextVarProvider(dict, copy_policy="strict")

        container = StrictFlowContext()
        parent_context = container.context()
        parent_context["items"] = []

        try:
            with pytest.raises(ValueError, match="context key 'items'.*nested mutable"):
                _strict_timeout_probe()
        finally:
            parent_context.clear()

    @pytest.mark.asyncio
    async def test_async_node_timeout_raises(self):
        @node(name="slow_async", timeout=0.01)
        async def slow_async() -> str:
            await asyncio.sleep(0.05)
            return "done"

        with pytest.raises(NodeTimeoutException) as exc_info:
            await slow_async()

        assert exc_info.value.node_name == "slow_async"
        assert exc_info.value.timeout_seconds == 0.01

    @pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
    def test_invalid_timeout_raises(self, timeout):
        with pytest.raises(ValueError, match="timeout"):
            node(timeout=timeout)

    @pytest.mark.asyncio
    async def test_timeout_is_total_budget_for_retrying_node(self):
        call_count = 0

        class TempError(Exception):
            retryable = True

        @node(
            timeout=0.03,
            retry_count=3,
            retry_delay=0.02,
            exception_types=(TempError,),
            enable_retry=True,
        )
        async def flaky_node() -> None:
            nonlocal call_count
            call_count += 1
            raise TempError("retry")

        with pytest.raises(NodeTimeoutException):
            await flaky_node()

        assert call_count == 2


class TestNodeDecoratorWithDI:
    @pytest.mark.parametrize("async_node", [False, True])
    def test_dependencies_keep_identity_while_business_inputs_are_validated(
        self, async_node
    ):
        container = BaseFlowContext()
        original = container.context()

        def write(value: int, state: dict = Provide[BaseFlowContext.context]) -> int:
            assert state is original
            state["value"] = value
            return value

        async def async_write(
            value: int, state: dict = Provide[BaseFlowContext.context]
        ) -> int:
            return write(value, state)

        writer = node(async_write if async_node else write)

        @node
        def read(value: int, state: dict = Provide[BaseFlowContext.context]) -> int:
            assert state is original
            assert state["value"] == value
            return state["value"]

        container.wire(modules=[__name__])
        try:
            flow = writer.then(read)
            assert flow("12") == 12
            assert original == {"value": 12}
            with pytest.raises(ValidationInputException):
                flow("invalid")
            assert original == {"value": 12}
        finally:
            container.unwire()

    def test_postponed_annotated_dependency_injection(self):
        module = types.ModuleType("streamlet_future_di_test")
        sys.modules[module.__name__] = module
        try:
            exec(
                """from __future__ import annotations
from typing import Annotated
from dependency_injector.wiring import Provide
from streamlet import BaseFlowContext, node
container = BaseFlowContext()
@node
def read(state: Annotated[dict, Provide[BaseFlowContext.context]]) -> str:
    return state['key']
container.context()['key'] = 'value'
""",
                module.__dict__,
            )
            module.container.wire(modules=[module])
            assert module.read() == "value"
        finally:
            if hasattr(module, "container"):
                module.container.unwire()
            sys.modules.pop(module.__name__, None)

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

    def test_decorated_node_preserves_function_metadata_and_signature(self):
        def my_func(x: int, label: str = "value") -> str:
            """Build a labelled value."""
            return f"{label}:{x}"

        decorated = node(name="custom_name")(my_func)

        assert decorated.name == "custom_name"
        assert decorated.__name__ == "my_func"
        assert decorated.__wrapped__ is my_func
        assert decorated.__doc__ == "Build a labelled value."
        assert decorated.__annotations__ == {"x": int, "label": str, "return": str}
        assert str(inspect.signature(decorated)) == (
            "(x: int, label: str = 'value') -> str"
        )

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
