"""End-to-end integration tests for Streamlet workflows."""

import asyncio
import threading
from pathlib import Path

import pytest
from dependency_injector.wiring import Provide

from streamlet import BaseFlowContext, ParallelResult, node
from tests.conftest import increment


def test_public_streamlet_import_uses_workspace_source():
    import streamlet

    expected_path = Path(__file__).resolve().parents[1] / "src/streamlet/__init__.py"
    assert streamlet.__file__ is not None
    assert Path(streamlet.__file__).resolve() == expected_path


class TestFullPipeline:
    """Complete data processing pipeline: extract → transform → load."""

    @pytest.mark.asyncio
    async def test_etl_pipeline(self):
        @node
        async def extract(source: str) -> list:
            await asyncio.sleep(0.01)
            return [{"id": i, "value": i * 10} for i in range(5)]

        @node
        def transform(items: list) -> list:
            return [{"id": item["id"], "doubled": item["value"] * 2} for item in items]

        @node
        def load(items: list) -> dict:
            return {"loaded": len(items), "data": items}

        pipeline = extract.then(transform).then(load)
        result = await pipeline("database")
        assert result["loaded"] == 5
        assert result["data"][0]["doubled"] == 0
        assert result["data"][4]["doubled"] == 80

    def test_sync_pipeline(self):
        @node
        def step1(x: int) -> int:
            return x + 1

        @node
        def step2(x: int) -> int:
            return x * 3

        @node
        def step3(x: int) -> str:
            return f"result:{x}"

        pipeline = step1.then(step2).then(step3)
        result = pipeline(5)
        assert result == "result:18"  # (5+1)*3=18


class TestFanOutFanInWorkflow:
    """Parallel processing with aggregation."""

    def test_parallel_processing_with_aggregation(self):
        @node
        def generate_data(x: int) -> dict:
            return {"value": x}

        @node
        def double(data: dict) -> int:
            return data["value"] * 2

        @node
        def triple(data: dict) -> int:
            return data["value"] * 3

        @node
        def collect(results: dict) -> dict:
            values = [r.result for r in results.values() if r.success]
            return {"count": len(values), "values": sorted(values)}

        pipeline = generate_data.fan_out_to([double, triple], executor="thread").fan_in(
            collect
        )
        result = pipeline(5)
        assert result["count"] == 2
        assert 10 in result["values"]
        assert 15 in result["values"]


class TestConditionalWorkflow:
    """Conditional branching workflows."""

    def test_conditional_routing(self):
        container = BaseFlowContext()

        @node
        def classify(data: dict) -> str:
            return "high" if data["score"] >= 80 else "low"

        @node
        def handle_high(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"level": "A", "original": state["score"]}

        @node
        def handle_low(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"level": "B", "original": state["score"]}

        container.wire(modules=[__name__])

        workflow = classify.branch_on({"high": handle_high, "low": handle_low})

        container.context()["score"] = 90
        high_result = workflow({"score": 90})
        assert high_result["level"] == "A"

        container.context()["score"] = 50
        low_result = workflow({"score": 50})
        assert low_result["level"] == "B"


class TestConcurrentSafety:
    """Verify thread safety of the framework."""

    def test_parallel_execution_thread_safety(self):
        @node
        def heavy_task(x: int) -> int:
            total = 0
            for i in range(x):
                total += i
            return total

        flow = heavy_task.fan_out_to([heavy_task, heavy_task], executor="thread")
        results = flow(1000)
        assert len(results) == 2
        for r in results.values():
            assert isinstance(r, ParallelResult)
            assert r.success is True

    def test_state_isolation_in_threads(self):
        container = BaseFlowContext()

        @node
        def stateful_node(
            x: int, state: dict = Provide[BaseFlowContext.context]
        ) -> int:
            state[f"key_{x}"] = x
            return len(state)

        container.wire(modules=[__name__])

        results = []
        errors = []

        def run_in_thread(val):
            try:
                results.append(stateful_node(val))
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=run_in_thread, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            pytest.fail(f"Thread errors: {errors}")
        assert len(results) == 3


class TestErrorRecovery:
    """Error handling in complex workflows."""

    def test_partial_failure_in_parallel(self):
        @node
        def good_node(x: int) -> int:
            return x * 2

        @node
        def bad_node(x: int) -> int:
            raise ValueError("simulated failure")

        @node
        def aggregator(results: dict) -> dict:
            successful = [r.result for r in results.values() if r.success]
            failed = [r.error for r in results.values() if not r.success]
            return {"success_count": len(successful), "fail_count": len(failed)}

        flow = good_node.fan_out_to([good_node, bad_node], executor="thread").fan_in(
            aggregator
        )
        result = flow(10)
        assert result["success_count"] == 1
        assert result["fail_count"] == 1


class TestLargeDataHandling:
    """Performance with larger datasets."""

    def test_many_iterations_repeat(self):
        flow = increment.repeat(100)
        result = flow({"value": 0})
        assert result.args == ({"value": 100},)

    def test_many_targets_fan_out(self):
        @node
        def identity(x: int) -> int:
            return x

        targets = [identity] * 20
        flow = identity.fan_out_to(targets, executor="thread")
        results = flow(42)
        assert len(results) == 20
        for r in results.values():
            assert r.success is True
            assert r.result == 42
