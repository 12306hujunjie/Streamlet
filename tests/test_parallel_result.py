"""Tests for ParallelResult dataclass."""

from src.streamlet import ParallelResult


class TestParallelResult:
    def test_success_result(self):
        result = ParallelResult(
            node_name="test_node", success=True, result=42, execution_time=0.1
        )
        assert result.node_name == "test_node"
        assert result.success is True
        assert result.result == 42
        assert result.error is None
        assert result.error_traceback is None
        assert result.execution_time == 0.1

    def test_failure_result(self):
        result = ParallelResult(
            node_name="failed_node",
            success=False,
            error="something went wrong",
            error_traceback="Traceback...",
            execution_time=0.5,
        )
        assert result.node_name == "failed_node"
        assert result.success is False
        assert result.result is None
        assert result.error == "something went wrong"
        assert result.error_traceback == "Traceback..."
        assert result.execution_time == 0.5

    def test_default_values(self):
        result = ParallelResult(node_name="minimal", success=True)
        assert result.result is None
        assert result.error is None
        assert result.error_traceback is None
        assert result.execution_time is None
