"""Tests for Node.fan_in() aggregation and Node.fan_out_in() combined operation."""

import pytest

from streamlet import ParallelResult, fan_out_args, node
from tests.conftest import add_five, aggregate_sum, multiply, source_data


class TestFanInBasic:
    def test_fan_out_then_fan_in(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="thread").fan_in(
            aggregate_sum
        )
        result = flow(10)
        assert result["total"] == 35  # 20 + 15
        assert result["count"] == 2

    @pytest.mark.asyncio
    async def test_async_fan_out_then_fan_in(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="async").fan_in(
            aggregate_sum
        )
        result = await flow(10)
        assert result["total"] == 35
        assert result["count"] == 2

    def test_fan_in_receives_parallel_result_dict(self):
        received = []

        @node
        def inspector(data: dict) -> dict:
            received.append(data)
            return {"ok": True}

        flow = source_data.fan_out_to([multiply], executor="thread").fan_in(inspector)
        flow(10)
        assert len(received) > 0
        data = received[0]
        for key, val in data.items():
            assert isinstance(val, ParallelResult)

    def test_fan_out_args_then_fan_in(self):
        @node
        def source() -> object:
            return fan_out_args({"value": 2}, {"value": 3})

        @node
        def double_value(value: int) -> int:
            return value * 2

        @node
        def square_value(value: int) -> int:
            return value**2

        @node
        def collect(results: dict) -> list[int]:
            return sorted(r.result for r in results.values() if r.success)

        result = source.fan_out_to(
            [double_value, square_value], executor="thread"
        ).fan_in(collect)()

        assert result == [4, 9]


class TestFanInEdgeCases:
    def test_single_target_aggregation(self):
        flow = source_data.fan_out_to([multiply], executor="thread").fan_in(
            aggregate_sum
        )
        result = flow(10)
        assert result["total"] == 20
        assert result["count"] == 1

    def test_aggregation_with_failures(self):
        @node
        def failing_node(data: dict) -> int:
            raise ValueError("fail")

        flow = source_data.fan_out_to(
            [multiply, failing_node], executor="thread"
        ).fan_in(aggregate_sum)
        result = flow(10)
        assert result["count"] == 1  # only multiply succeeded
        assert result["total"] == 20


class TestFanOutIn:
    """fan_out_in() 等价于 fan_out_to().fan_in() 的语法糖验证。"""

    def test_equivalent_to_fan_out_then_fan_in(self):
        result1 = source_data.fan_out_in(
            [multiply, add_five], aggregate_sum, executor="thread"
        )(10)
        result2 = source_data.fan_out_to(
            [multiply, add_five], executor="thread"
        ).fan_in(aggregate_sum)(10)
        assert result1 == result2

    def test_fan_out_args_with_fan_out_in(self):
        @node
        def source() -> object:
            return fan_out_args({"value": 5}, {"value": 7})

        @node
        def minus_one(value: int) -> int:
            return value - 1

        @node
        def plus_one(value: int) -> int:
            return value + 1

        @node
        def collect(results: dict) -> list[int]:
            return sorted(r.result for r in results.values() if r.success)

        result = source.fan_out_in([minus_one, plus_one], collect, executor="thread")()

        assert result == [4, 8]

    def test_fan_out_in_rejects_non_node_targets_with_target_name(self):
        with pytest.raises(TypeError, match=r"targets\[0\] must be a Node"):
            source_data.fan_out_in([object()], aggregate_sum)
