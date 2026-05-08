"""Tests for Node.repeat() composition."""

import pytest

from src.streamlet import LoopControlException, Node, node


@node
def increment(data: dict) -> dict:
    val = data.get("value", 0)
    return {"value": val + 1}


@node
def double_value(data: dict) -> dict:
    return {"value": data["value"] * 2}


@node
def failing_node(data: dict) -> dict:
    raise ValueError("iteration failed")


class TestRepeatBasic:
    def test_repeat_n_times(self):
        flow = increment.repeat(3)
        result = flow({"value": 0})
        assert result["value"] == 3

    def test_repeat_once(self):
        flow = increment.repeat(1)
        result = flow({"value": 0})
        assert result["value"] == 1

    def test_repeat_preserves_data_flow(self):
        flow = double_value.repeat(3)
        result = flow({"value": 1})
        assert result["value"] == 8  # 1*2=2, 2*2=4, 4*2=8

    def test_repeat_returns_node(self):
        flow = increment.repeat(3)
        assert isinstance(flow, Node)

    def test_repeat_zero_raises(self):
        with pytest.raises(ValueError, match="Repeat times"):
            increment.repeat(0)

    def test_repeat_negative_raises(self):
        with pytest.raises(ValueError, match="Repeat times"):
            increment.repeat(-1)

    def test_repeat_non_integer_raises(self):
        with pytest.raises(TypeError, match="times"):
            increment.repeat("invalid")  # type: ignore[arg-type]


class TestRepeatErrorHandling:
    def test_stop_on_error_true(self):
        flow = failing_node.repeat(3, stop_on_error=True)
        with pytest.raises(LoopControlException):
            flow({"value": 0})

    def test_stop_on_error_false_continues(self):
        flow = failing_node.repeat(3, stop_on_error=False)
        result = flow({"value": 0})
        # When continuing on error, last successful result is returned (None for all-failure)
        assert result is None


class TestRepeatWithState:
    def test_accumulation_over_iterations(self):
        @node
        def accumulate(data: dict) -> dict:
            items = data.get("items", [])
            count = len(items) + 1
            return {"items": items + [count], "count": count}

        flow = accumulate.repeat(5)
        result = flow({})
        assert result["count"] == 5
        assert result["items"] == [1, 2, 3, 4, 5]

    def test_repeat_with_initial_data(self):
        flow = double_value.repeat(2)
        result = flow({"value": 3})
        assert result["value"] == 12  # 3*2=6, 6*2=12


class TestRepeatAsync:
    @pytest.mark.asyncio
    async def test_async_repeat(self):
        @node
        async def async_increment(data: dict) -> dict:
            return {"value": data.get("value", 0) + 1}

        flow = async_increment.repeat(3)
        result = await flow({"value": 0})
        assert result["value"] == 3

    @pytest.mark.asyncio
    async def test_async_repeat_stop_on_error(self):
        @node
        async def async_failing(data: dict) -> dict:
            raise ValueError("async iteration failed")

        flow = async_failing.repeat(2, stop_on_error=True)
        with pytest.raises(LoopControlException):
            await flow({"value": 0})
