"""Tests for Node.fan_out_in() combined fan-out and fan-in."""

import pytest

from src.streamlet import node


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


class TestFanOutIn:
    def test_thread_executor(self):
        flow = source_data.fan_out_in(
            [multiply, add_five], aggregate, executor="thread"
        )
        result = flow(10)
        assert result["total"] == 35
        assert result["count"] == 2

    @pytest.mark.asyncio
    async def test_async_executor(self):
        flow = source_data.fan_out_in([multiply, add_five], aggregate, executor="async")
        result = await flow(10)
        assert result["total"] == 35
        assert result["count"] == 2

    def test_auto_executor(self):
        flow = source_data.fan_out_in([multiply, add_five], aggregate, executor="auto")
        result = flow(10)
        assert result["total"] == 35

    def test_equivalent_to_fan_out_then_fan_in(self):
        result1 = source_data.fan_out_in(
            [multiply, add_five], aggregate, executor="thread"
        )(10)
        result2 = source_data.fan_out_to(
            [multiply, add_five], executor="thread"
        ).fan_in(aggregate)(10)
        assert result1 == result2
