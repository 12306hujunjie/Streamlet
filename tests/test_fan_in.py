"""Tests for Node.fan_in() aggregation and Node.fan_out_in() combined operation."""

import pytest

from src.streamlet import ParallelResult, node
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
