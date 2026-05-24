"""Tests for BaseFlowContext."""

import threading

from src.streamlet import BaseFlowContext


class TestBaseFlowContext:
    def test_container_creation(self):
        container = BaseFlowContext()
        assert container is not None

    def test_state_thread_local_singleton(self):
        container = BaseFlowContext()
        state = container.state()
        assert isinstance(state, dict)
        state["x"] = 42
        assert container.state()["x"] == 42

    def test_state_isolation_between_containers(self):
        c1 = BaseFlowContext()
        c2 = BaseFlowContext()
        c1.state()["x"] = 1
        c2.state()["x"] = 2
        assert c1.state()["x"] == 1
        assert c2.state()["x"] == 2

    def test_state_thread_isolation(self):
        container = BaseFlowContext()
        container.state()["main"] = "main_value"
        results = []

        def thread_func():
            results.append(container.state().get("main"))

        t = threading.Thread(target=thread_func)
        t.start()
        t.join()
        # ThreadLocalSingleton gives each thread its own state
        assert results[0] is None

    def test_shared_data_singleton(self):
        container = BaseFlowContext()
        container.shared_data()["shared"] = 42
        assert container.shared_data()["shared"] == 42

    def test_async_state_provider(self):
        container = BaseFlowContext()
        state = container.async_state()
        assert isinstance(state, dict)

    def test_context_provider(self):
        container = BaseFlowContext()
        ctx = container.context()
        assert isinstance(ctx, dict)
