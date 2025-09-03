"""Tests for standalone composition functions (non-fluent API)."""

import pytest
from dependency_injector.wiring import Provide

from src.aetherflow import (
    BaseFlowContext,
    Node,
    conditional_composition,
    node,
    parallel_fan_in,
    parallel_fan_out,
    parallel_fan_out_in,
    repeat_composition,
    sequential_composition,
)


@node
def double(x: int) -> int:
    return x * 2


@node
def add_ten(x: int) -> int:
    return x + 10


@node
def check_positive(x: int) -> bool:
    return x >= 0


class TestSequentialComposition:
    def test_basic(self):
        flow = sequential_composition(double, add_ten)
        assert isinstance(flow, Node)
        assert flow(5) == 20

    @pytest.mark.asyncio
    async def test_with_async_nodes(self):
        @node
        async def async_add(x: int) -> int:
            return x + 10

        flow = sequential_composition(double, async_add)
        assert await flow(5) == 20


class TestParallelFanOut:
    def test_basic(self):
        flow = parallel_fan_out(double, [add_ten], executor="thread")
        assert isinstance(flow, Node)
        results = flow(5)
        assert len(results) == 1

    def test_empty_targets_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            parallel_fan_out(double, [])

    def test_invalid_executor_raises(self):
        with pytest.raises(ValueError):
            parallel_fan_out(double, [add_ten], executor="invalid")


class TestParallelFanIn:
    def test_basic(self):
        @node
        def aggregate(results: dict) -> int:
            return sum(r.result for r in results.values())

        fan_out = parallel_fan_out(double, [add_ten], executor="thread")
        flow = parallel_fan_in(fan_out, aggregate)
        assert flow(5) == 20  # add_ten(5) = 15

    def test_returns_node(self):
        @node
        def aggregate(results: dict) -> int:
            return 0

        fan_out = parallel_fan_out(double, [add_ten], executor="thread")
        flow = parallel_fan_in(fan_out, aggregate)
        assert isinstance(flow, Node)


class TestParallelFanOutIn:
    def test_basic(self):
        @node
        def aggregate(results: dict) -> dict:
            return {"total": sum(r.result for r in results.values())}

        flow = parallel_fan_out_in(double, [add_ten], aggregate, executor="thread")
        result = flow(5)
        assert isinstance(result, dict)


class TestConditionalComposition:
    def test_basic(self):
        container = BaseFlowContext()

        @node
        def handle_positive(state: dict = Provide[BaseFlowContext.state]) -> str:
            return f"positive:{state['value']}"

        @node
        def handle_negative(state: dict = Provide[BaseFlowContext.state]) -> str:
            return f"negative:{state['value']}"

        container.wire(modules=[__name__])
        container.state()["value"] = 42

        flow = conditional_composition(
            check_positive, {True: handle_positive, False: handle_negative}
        )
        assert flow(42) == "positive:42"

        container.state()["value"] = -5
        assert flow(-5) == "negative:-5"

    def test_returns_node(self):
        container = BaseFlowContext()

        @node
        def handle(state: dict = Provide[BaseFlowContext.state]) -> str:
            return "ok"

        container.wire(modules=[__name__])

        flow = conditional_composition(check_positive, {True: handle, False: handle})
        assert isinstance(flow, Node)


class TestRepeatComposition:
    def test_basic(self):
        @node
        def inc(x: dict) -> dict:
            return {"value": x.get("value", 0) + 1}

        flow = repeat_composition(inc, times=3)
        result = flow({"value": 0})
        assert result["value"] == 3

    def test_times_zero_raises(self):
        with pytest.raises(ValueError, match="Repeat times"):
            repeat_composition(double, times=0)

    def test_non_node_raises(self):
        with pytest.raises(TypeError, match="node must be a Node"):
            repeat_composition("not_a_node", times=3)  # type: ignore[arg-type]

    def test_non_integer_times_raises(self):
        with pytest.raises(TypeError, match="times"):
            repeat_composition(double, "invalid")  # type: ignore[arg-type]
