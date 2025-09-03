"""Tests for Node.fan_in() aggregation."""

import pytest

from src.aetherflow import ParallelResult, node


@node
def source_data(x: int) -> dict:
    return {"value": x}


@node
def multiply(data: dict) -> int:
    return data["value"] * 2


@node
def add_five(data: dict) -> int:
    return data["value"] + 5


@node
def aggregate(parallel_results: dict) -> dict:
    successful = [r.result for r in parallel_results.values() if r.success]
    return {"total": sum(successful), "count": len(successful)}


class TestFanInBasic:
    def test_fan_out_then_fan_in(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="thread").fan_in(
            aggregate
        )
        result = flow(10)
        assert result["total"] == 35  # 20 + 15
        assert result["count"] == 2

    @pytest.mark.asyncio
    async def test_async_fan_out_then_fan_in(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="async").fan_in(
            aggregate
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
        flow = source_data.fan_out_to([multiply], executor="thread").fan_in(aggregate)
        result = flow(10)
        assert result["total"] == 20
        assert result["count"] == 1

    def test_aggregation_with_failures(self):
        @node
        def failing_node(data: dict) -> int:
            raise ValueError("fail")

        flow = source_data.fan_out_to(
            [multiply, failing_node], executor="thread"
        ).fan_in(aggregate)
        result = flow(10)
        assert result["count"] == 1  # only multiply succeeded
        assert result["total"] == 20
