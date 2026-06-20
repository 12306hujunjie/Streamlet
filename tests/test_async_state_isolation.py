"""Tests for coroutine-safe state isolation in async fan-out execution."""

import asyncio

import pytest
from dependency_injector.wiring import Provide

from streamlet import BaseFlowContext, node


@pytest.mark.asyncio
async def test_async_fan_out_isolates_context_between_branches():
    """fan_out_to(executor='async') 下，每个分支应拥有独立的 context 副本。

    关键点：
    - 上游节点写入的 context 需要在各分支可见（作为初始值）
    - 分支之间的写入不能互相污染
    """

    container = BaseFlowContext()

    @node
    async def source(x: int) -> int:
        return x

    @node
    async def branch_a(x: int, state: dict = Provide[BaseFlowContext.context]) -> str:
        assert state["value"] == 123
        state["branch"] = "a"
        # yield，让另一个分支有机会覆盖共享 dict（若隔离失败）
        await asyncio.sleep(0.01)
        return state["branch"]

    @node
    async def branch_b(x: int, state: dict = Provide[BaseFlowContext.context]) -> str:
        assert state["value"] == 123
        state["branch"] = "b"
        await asyncio.sleep(0.01)
        return state["branch"]

    # wiring 必须在 @node 定义之后
    container.wire(modules=[__name__])

    # 在 async task 中预先写入 context，后续 fan-out 分支应继承且彼此隔离
    container.context()["value"] = 123

    flow = source.fan_out_to([branch_a, branch_b], executor="async")
    results = await flow(123)

    assert results["branch_a"].result == "a"
    assert results["branch_b"].result == "b"
