"""Tests for Node.fan_out_to() parallel distribution."""

import asyncio
import time

import pytest

from streamlet import FanOutArgs, ParallelResult, fan_out_args, node
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

    def test_async_executor_runs_from_sync_context(self):
        flow = source_data.fan_out_to([multiply, add_five], executor="async")
        results = flow(10)
        assert sorted(r.result for r in results.values()) == [15, 20]

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

    @pytest.mark.asyncio
    async def test_auto_executor_with_fan_out_args_and_mixed_targets(self):
        @node
        def source(user_id: str):
            return fan_out_args(
                {"user_id": user_id, "limit": 3},
                {"user_id": user_id, "limit": 4},
            )

        @node
        def sync_fetch(user_id: str, limit: int) -> tuple[str, int]:
            return user_id, limit

        @node
        async def async_fetch(user_id: str, limit: int) -> tuple[str, int]:
            return user_id, limit

        flow = source.fan_out_to([sync_fetch, async_fetch], executor="auto")

        results = await flow("u-3")

        assert results["sync_fetch"].result == ("u-3", 3)
        assert results["async_fetch"].result == ("u-3", 4)

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

    @pytest.mark.asyncio
    async def test_async_source_thread_executor_runs_in_existing_event_loop(self):
        @node
        async def async_source(x: int) -> dict:
            return {"value": x * 2}

        flow = async_source.fan_out_to([multiply], executor="thread")

        results = await flow(10)

        assert results["multiply"].success is True
        assert results["multiply"].result == 40

    @pytest.mark.asyncio
    async def test_async_source_thread_executor_does_not_block_event_loop(self):
        @node
        async def async_source(x: int) -> dict:
            return {"value": x}

        @node
        def slow_target(data: dict) -> int:
            time.sleep(0.1)
            return data["value"]

        flow = async_source.fan_out_to([slow_target], executor="thread")

        start = time.perf_counter()
        first, second = await asyncio.gather(flow(1), flow(2))
        elapsed = time.perf_counter() - start

        assert first["slow_target"].result == 1
        assert second["slow_target"].result == 2
        assert elapsed < 0.18


class TestFanOutArgs:
    def test_source_can_route_kwargs_to_matching_targets(self):
        @node
        def source(user_id: str):
            return fan_out_args(
                {"user_id": user_id, "limit": 10},
                {"user_id": user_id, "include_archived": False},
            )

        @node
        def fetch_orders(user_id: str, limit: int) -> tuple[str, int]:
            return user_id, limit

        @node
        def fetch_profile(user_id: str, include_archived: bool) -> tuple[str, bool]:
            return user_id, include_archived

        flow = source.fan_out_to([fetch_orders, fetch_profile], executor="thread")

        results = flow("u-1")

        assert results["fetch_orders"].result == ("u-1", 10)
        assert results["fetch_profile"].result == ("u-1", False)

    @pytest.mark.asyncio
    async def test_async_fan_out_args_route_kwargs_to_matching_targets(self):
        @node
        async def source(user_id: str):
            return fan_out_args(
                {"user_id": user_id, "limit": 5},
                {"user_id": user_id, "include_archived": True},
            )

        @node
        async def fetch_orders(user_id: str, limit: int) -> tuple[str, int]:
            return user_id, limit

        @node
        async def fetch_profile(
            user_id: str, include_archived: bool
        ) -> tuple[str, bool]:
            return user_id, include_archived

        flow = source.fan_out_to([fetch_orders, fetch_profile], executor="async")

        results = await flow("u-2")

        assert results["fetch_orders"].result == ("u-2", 5)
        assert results["fetch_profile"].result == ("u-2", True)

    def test_fan_out_args_requires_one_input_per_target(self):
        @node
        def source():
            return fan_out_args({"value": 1})

        @node
        def left(value: int) -> int:
            return value

        @node
        def right(value: int) -> int:
            return value

        flow = source.fan_out_to([left, right], executor="thread")

        with pytest.raises(ValueError, match="expected 2 fan-out inputs, got 1"):
            flow()

    def test_fan_out_args_rejects_non_dict_items(self):
        with pytest.raises(TypeError, match="fan_out_args items must be dict"):
            fan_out_args({"value": 1}, ["not", "a", "dict"])

    def test_fan_out_args_constructor_rejects_non_dict_items(self):
        with pytest.raises(TypeError, match="fan_out_args items must be dict"):
            FanOutArgs(({"value": 1}, ["not", "a", "dict"]))

    def test_fan_out_args_constructor_copies_items(self):
        item = {"value": 1}

        args = FanOutArgs((item,))
        item["value"] = 2

        assert args.items[0]["value"] == 1

    def test_plain_list_of_dicts_is_still_broadcast_as_single_input(self):
        payload = [{"value": 1}, {"value": 2}]

        @node
        def source() -> list[dict]:
            return payload

        @node
        def count(items: list[dict]) -> int:
            return len(items)

        flow = source.fan_out_to([count], executor="thread")

        results = flow()

        assert results["count"].result == 2
