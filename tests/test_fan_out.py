"""Tests for Node.fan_out_to() parallel distribution."""

import pytest

from src.streamlet import ParallelResult, node
from tests.conftest import add_five, multiply, source_data


@node
async def async_multiply(data: dict) -> int:
    return data["value"] * 3


@node
def failing_target(data: dict) -> int:
    raise ValueError("target failed")


class TestFanOutBasic:
    def test_two_targets_thread(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="thread")
        results = flow(10)
        assert len(results) == 2
        result_values = [r.result for r in results.values()]
        assert 20 in result_values  # 10 * 2
        assert 15 in result_values  # 10 + 5

    @pytest.mark.asyncio
    async def test_two_targets_async(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="async")
        results = await flow(10)
        assert len(results) == 2
        result_values = [r.result for r in results.values()]
        assert 20 in result_values
        assert 15 in result_values

    def test_single_target(self):
        flow = source_data.fan_out_to([multiply], executor="thread")
        results = flow(10)
        assert len(results) == 1
        result = next(iter(results.values()))
        assert isinstance(result, ParallelResult)
        assert result.result == 20

    def test_successful_results_have_execution_time(self):
        flow = source_data.fan_out_to([multiply], executor="thread")
        results = flow(10)
        for r in results.values():
            assert r.success is True
            assert r.execution_time is not None
            assert r.execution_time >= 0


class TestFanOutExecutorTypes:
    def test_auto_executor_with_sync_nodes(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="auto")
        results = flow(10)
        assert sorted(r.result for r in results.values()) == [15, 20]

    def test_invalid_executor_raises(self):
        with pytest.raises(ValueError, match="Only 'thread', 'async', and 'auto'"):
            source_data.fan_out_to([multiply], executor="process")

    def test_empty_targets_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            source_data.fan_out_to([])


class TestFanOutErrorHandling:
    def test_partial_failure(self):
        flow = source_data.fan_out_to([multiply, failing_target], executor="thread")
        results = flow(10)
        assert len(results) == 2
        success_count = sum(1 for r in results.values() if r.success)
        fail_count = sum(1 for r in results.values() if not r.success)
        assert success_count == 1
        assert fail_count == 1

    def test_failure_result_has_error_info(self):
        flow = source_data.fan_out_to([failing_target], executor="thread")
        results = flow(10)
        failed = [r for r in results.values() if not r.success][0]
        assert failed.error == "target failed"
        assert failed.error_traceback is not None

    def test_all_failure(self):
        flow = source_data.fan_out_to(
            [failing_target, failing_target], executor="thread"
        )
        results = flow(10)
        all_failed = all(not r.success for r in results.values())
        assert all_failed


class TestFanOutWithAsyncSource:
    @pytest.mark.asyncio
    async def test_async_source_async_executor(self):
        @node
        async def async_source(x: int) -> dict:
            return {"value": x * 2}

        flow = async_source.fan_out_to([async_multiply], executor="async")
        results = await flow(10)
        result_value = list(results.values())[0].result
        assert result_value == 60  # source: 10*2=20, multiply: 20*3=60
