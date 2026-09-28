"""Public workflows: isolated requests, partial failure, recovery and final effects."""

import asyncio

import pytest
from dependency_injector.wiring import Provide

from streamlet import BaseFlowContext, fan_out_args, node


@pytest.mark.parametrize("executor", ["thread", "async", "auto"])
async def test_concurrent_workflows_recover_and_publish_once(executor):
    container = BaseFlowContext()
    attempts = []
    published = []

    @node
    async def source(
        request_id: str, value: int, state: dict = Provide[BaseFlowContext.context]
    ):
        state["request_id"] = request_id
        return fan_out_args({"value": value}, {"value": value})

    @node(enable_retry=True, retry_count=1, retry_delay=0)
    def compute(value: int, state: dict = Provide[BaseFlowContext.context]) -> int:
        attempts.append((state["request_id"], value))
        if "attempted" not in state:
            state["attempted"] = True
            raise ConnectionError("transient calculation failure")
        return value + 1

    @node
    async def optional_service(value: int) -> int:
        await asyncio.sleep(0)
        raise ValueError("optional service unavailable")

    @node
    def collect(results: dict, state: dict = Provide[BaseFlowContext.context]) -> bool:
        successful = [r.result for r in results.values() if r.success]
        failed = [r for r in results.values() if not r.success]
        assert len(successful) == 1
        assert len(failed) == 1
        assert failed[0].error == "optional service unavailable"
        assert "attempted" not in state
        state["result"] = successful[0]
        return state["result"] > 0

    @node
    def publish(state: dict = Provide[BaseFlowContext.context]) -> str:
        published.append((state["request_id"], state["result"]))
        return f"published:{state['request_id']}:{state['result']}"

    @node
    def skip(state: dict = Provide[BaseFlowContext.context]) -> str:
        return f"skipped:{state['request_id']}:{state['result']}"

    container.wire(modules=[__name__])
    try:
        flow = source.fan_out_in(
            [compute.repeat(3, stop_on_error=True), optional_service],
            collect,
            executor=executor,
            max_workers=2,
        ).branch_on({True: publish, False: skip})
        assert await asyncio.gather(flow("a", 10), flow("b", -10)) == [
            "published:a:13",
            "skipped:b:-7",
        ]
        assert published == [("a", 13)]
        assert [value for request, value in attempts if request == "a"] == [
            10,
            10,
            11,
            12,
        ]
        assert [value for request, value in attempts if request == "b"] == [
            -10,
            -10,
            -9,
            -8,
        ]
        assert container.context() == {}
    finally:
        container.unwire()


async def test_cancellation_stops_repeat_and_retry_without_publishing():
    started = asyncio.Event()
    calls = []
    published = []

    @node(enable_retry=True, retry_count=3, retry_delay=0)
    async def wait_for_service(value: int) -> int:
        calls.append(value)
        started.set()
        await asyncio.Event().wait()
        return value

    @node
    def publish(value: int) -> int:
        published.append(value)
        return value

    flow = wait_for_service.repeat(3).then(publish)
    task = asyncio.create_task(flow(1))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert calls == [1]
    assert published == []
