"""Tests for Node.repeat() composition."""

import pytest

from streamlet import CallArgs, LoopControlException, RepeatInputMode, call_args, node
from tests.conftest import increment


@node
def double_value(data: dict) -> CallArgs:
    return call_args({"value": data["value"] * 2})


@node
def failing_node(data: dict) -> dict:
    raise ValueError("iteration failed")


class TestRepeatBasic:
    def test_repeat_public_protocols_come_from_public_module(self):
        assert RepeatInputMode.__module__ == "streamlet.types"
        assert CallArgs.__module__ == "streamlet.types"
        assert call_args.__module__ == "streamlet.types"

    def test_repeat_n_times(self):
        flow = increment.repeat(3)
        result = flow({"value": 0})
        assert result == call_args({"value": 3})

    def test_repeat_once(self):
        flow = increment.repeat(1)
        result = flow({"value": 0})
        assert result == call_args({"value": 1})

    def test_repeat_preserves_data_flow(self):
        flow = double_value.repeat(3)
        result = flow({"value": 1})
        assert result == call_args({"value": 8})  # 1*2=2, 2*2=4, 4*2=8

    def test_repeat_zero_raises(self):
        with pytest.raises(ValueError, match="Repeat times"):
            increment.repeat(0)

    def test_repeat_negative_raises(self):
        with pytest.raises(ValueError, match="Repeat times"):
            increment.repeat(-1)

    def test_repeat_non_integer_raises(self):
        with pytest.raises(TypeError, match="times"):
            increment.repeat("invalid")  # type: ignore[arg-type]

    @pytest.mark.parametrize("times", [True, False])
    def test_repeat_bool_times_raises(self, times):
        with pytest.raises(TypeError, match="times"):
            increment.repeat(times)


class TestRepeatErrorHandling:
    def test_stop_on_error_true(self):
        flow = failing_node.repeat(3, stop_on_error=True)
        with pytest.raises(LoopControlException) as exc_info:
            flow({"value": 0})
        assert exc_info.value.node_name == "failing_node"
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_stop_on_error_false_continues(self):
        flow = failing_node.repeat(3, stop_on_error=False)
        result = flow({"value": 0})
        # When continuing on error, last successful result is returned.
        # For all-failure runs that means None.
        assert result is None


class TestRepeatWithState:
    def test_accumulation_over_iterations(self):
        @node
        def accumulate(data: dict) -> CallArgs:
            items = data.get("items", [])
            count = len(items) + 1
            return call_args({"items": items + [count], "count": count})

        flow = accumulate.repeat(5)
        result = flow({})
        assert result == call_args({"items": [1, 2, 3, 4, 5], "count": 5})

    def test_repeat_with_initial_data(self):
        flow = double_value.repeat(2)
        result = flow({"value": 3})
        assert result == call_args({"value": 12})  # 3*2=6, 6*2=12

    def test_previous_result_uses_call_args_args_and_kwargs(self):
        calls: list[tuple[tuple[int, ...], int]] = []

        @node
        def sum_values(*values: int, scale: int = 1):
            calls.append((values, scale))
            return call_args(sum(values) * scale + 1, scale=scale)

        flow = sum_values.repeat(3)

        assert flow(1, 2, scale=10) == call_args(3111, scale=10)
        assert calls == [
            ((1, 2), 10),
            ((31,), 10),
            ((311,), 10),
        ]

    def test_previous_result_passes_plain_result_to_next_iteration(self):
        @node
        def increment_value(value: int) -> int:
            return value + 1

        flow = increment_value.repeat(3)

        assert flow(0) == 3

    def test_previous_result_keeps_plain_dict_as_single_argument(self):
        @node
        def increment_payload(payload: dict[str, int]) -> dict[str, int]:
            return {"value": payload["value"] + 1}

        flow = increment_payload.repeat(2)

        assert flow({"value": 0}) == {"value": 2}

    def test_same_input_repeat_reuses_original_args_and_kwargs(self):
        calls: list[tuple[tuple[int, ...], int]] = []

        @node
        def sum_values(*values: int, scale: int = 1) -> int:
            calls.append((values, scale))
            return sum(values) * scale

        flow = sum_values.repeat(3, input_mode=RepeatInputMode.SAME_INPUT)

        assert flow(1, 2, scale=10) == 30
        assert calls == [
            ((1, 2), 10),
            ((1, 2), 10),
            ((1, 2), 10),
        ]

    def test_repeat_input_mode_must_be_enum(self):
        with pytest.raises(TypeError, match="input_mode"):
            increment.repeat(2, input_mode="same_input")  # type: ignore[arg-type]


class TestRepeatAsync:
    @pytest.mark.asyncio
    async def test_async_repeat(self):
        @node
        async def async_increment(data: dict) -> CallArgs:
            return call_args({"value": data.get("value", 0) + 1})

        flow = async_increment.repeat(3)
        result = await flow({"value": 0})
        assert result == call_args({"value": 3})

    @pytest.mark.asyncio
    async def test_async_previous_result_uses_call_args_args_and_kwargs(self):
        calls: list[tuple[tuple[int, ...], int]] = []

        @node
        async def async_sum_values(*values: int, scale: int = 1):
            calls.append((values, scale))
            return call_args(sum(values) * scale + 1, scale=scale)

        flow = async_sum_values.repeat(3)

        assert await flow(1, 2, scale=10) == call_args(3111, scale=10)
        assert calls == [
            ((1, 2), 10),
            ((31,), 10),
            ((311,), 10),
        ]

    @pytest.mark.asyncio
    async def test_async_previous_result_passes_plain_result_to_next_iteration(self):
        @node
        async def async_increment_value(value: int) -> int:
            return value + 1

        flow = async_increment_value.repeat(3)

        assert await flow(0) == 3

    @pytest.mark.asyncio
    async def test_async_same_input_repeat_reuses_original_args_and_kwargs(self):
        calls: list[tuple[tuple[int, ...], int]] = []

        @node
        async def async_sum_values(*values: int, scale: int = 1) -> int:
            calls.append((values, scale))
            return sum(values) * scale

        flow = async_sum_values.repeat(3, input_mode=RepeatInputMode.SAME_INPUT)

        assert await flow(1, 2, scale=10) == 30
        assert calls == [
            ((1, 2), 10),
            ((1, 2), 10),
            ((1, 2), 10),
        ]

    @pytest.mark.asyncio
    async def test_async_repeat_stop_on_error(self):
        @node
        async def async_failing(data: dict) -> dict:
            raise ValueError("async iteration failed")

        flow = async_failing.repeat(2, stop_on_error=True)
        with pytest.raises(LoopControlException) as exc_info:
            await flow({"value": 0})
        assert exc_info.value.node_name == "async_failing"
        assert isinstance(exc_info.value.__cause__, ValueError)
