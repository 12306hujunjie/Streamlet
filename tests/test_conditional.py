"""Tests for Node.branch_on() conditional branching.

Branch nodes receive data through dependency injection: @node's inject decorator
resolves Provide[BaseFlowContext.context] to actual values from the container.
"""

import pytest
from dependency_injector.wiring import Provide

from streamlet import BaseFlowContext, node


class TestBranchOnBoolean:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.container = BaseFlowContext()

    @pytest.mark.parametrize(
        ("score", "expected_status"),
        [(75, "passed"), (45, "failed")],
    )
    def test_branch_selected_by_condition_result(self, score, expected_status):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return data["score"] >= 60

        @node
        def handle_pass(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"status": "passed", "score": state["score"]}

        @node
        def handle_fail(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"status": "failed", "score": state["score"]}

        container.wire(modules=[__name__])
        container.context()["score"] = score

        flow = check_score.branch_on({True: handle_pass, False: handle_fail})
        result = flow({"score": score})
        assert result["status"] == expected_status

    def test_branch_receives_no_original_input_or_condition_result(self):
        @node
        def choose_branch(data: dict) -> str:
            return "selected"

        @node
        def handle_selected() -> str:
            return "handled"

        flow = choose_branch.branch_on({"selected": handle_selected})

        assert flow({"score": 75}) == "handled"

    def test_single_branch(self):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return data["score"] >= 60

        @node
        def handle_pass(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"status": "passed", "score": state["score"]}

        container.wire(modules=[__name__])
        container.context()["score"] = 75

        flow = check_score.branch_on({True: handle_pass})
        result = flow({"score": 75})
        assert result["status"] == "passed"

    def test_condition_error_propagates(self):
        container = self.container

        @node
        def failing_condition(data: dict) -> bool:
            raise ValueError("condition failed")

        @node
        def handle(state: dict = Provide[BaseFlowContext.context]) -> dict:
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
        def handle_pass(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"status": "passed"}

        container.wire(modules=[__name__])

        flow = check_score_str.branch_on({"pass": handle_pass})
        with pytest.raises(ValueError, match="No branch defined"):
            flow({"score": 35})

    @pytest.mark.asyncio
    async def test_async_branch(self):
        container = self.container

        @node
        def check_score(data: dict) -> bool:
            return data["score"] >= 60

        @node
        async def async_handle(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"status": "async_passed", "score": state["score"]}

        @node
        def sync_handle(state: dict = Provide[BaseFlowContext.context]) -> dict:
            return {"status": "failed", "score": state["score"]}

        container.wire(modules=[__name__])
        container.context()["score"] = 80

        flow = check_score.branch_on({True: async_handle, False: sync_handle})
        result = await flow({"score": 80})
        assert result["status"] == "async_passed"
