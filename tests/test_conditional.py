"""Tests for Node.branch_on() conditional branching.

Branch nodes receive data through dependency injection: @node's inject decorator
resolves Provide[BaseFlowContext.state] to actual values from the container.
"""

import pytest
from dependency_injector.wiring import Provide

from aetherflow import BaseFlowContext, Node, node


class TestBranchOnBoolean:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.container = BaseFlowContext()

    def test_true_branch_selected(self):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return data["score"] >= 60

        @node
        def handle_pass(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "passed", "score": state["score"]}

        @node
        def handle_fail(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "failed", "score": state["score"]}

        container.wire(modules=[__name__])
        container.state()["score"] = 75

        flow = check_score.branch_on({True: handle_pass, False: handle_fail})
        result = flow({"score": 75})
        assert result["status"] == "passed"

    def test_false_branch_selected(self):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return data["score"] >= 60

        @node
        def handle_pass(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "passed", "score": state["score"]}

        @node
        def handle_fail(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "failed", "score": state["score"]}

        container.wire(modules=[__name__])
        container.state()["score"] = 45

        flow = check_score.branch_on({True: handle_pass, False: handle_fail})
        result = flow({"score": 45})
        assert result["status"] == "failed"

    def test_single_branch(self):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return data["score"] >= 60

        @node
        def handle_pass(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "passed", "score": state["score"]}

        container.wire(modules=[__name__])
        container.state()["score"] = 75

        flow = check_score.branch_on({True: handle_pass})
        result = flow({"score": 75})
        assert result["status"] == "passed"

    def test_condition_error_propagates(self):
        container = self.container

        @node
        def failing_condition(data: dict) -> bool:
            raise ValueError("condition failed")

        @node
        def handle(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "ok"}

        container.wire(modules=[__name__])

        flow = failing_condition.branch_on({True: handle, False: handle})
        with pytest.raises(ValueError, match="condition failed"):
            flow({"score": 75})

    def test_unmatched_raises(self):
        container = self.container

        @node
        def check_score_str(data: dict) -> str:
            return "fail"

        @node
        def handle_pass(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "passed"}

        container.wire(modules=[__name__])

        flow = check_score_str.branch_on({"pass": handle_pass})
        with pytest.raises(ValueError, match="No branch defined"):
            flow({"score": 35})

    def test_returns_node(self):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return True

        @node
        def handle(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "ok"}

        container.wire(modules=[__name__])

        flow = check_score.branch_on({True: handle})
        assert isinstance(flow, Node)

    @pytest.mark.asyncio
    async def test_async_branch(self):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return data["score"] >= 60

        @node
        async def async_handle(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "async_passed", "score": state["score"]}

        @node
        def sync_handle(state: dict = Provide[BaseFlowContext.state]) -> dict:
            return {"status": "failed", "score": state["score"]}

        container.wire(modules=[__name__])
        container.state()["score"] = 80

        flow = check_score.branch_on({True: async_handle, False: sync_handle})
        result = await flow({"score": 80})
        assert result["status"] == "async_passed"
