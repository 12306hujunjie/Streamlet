"""Tests for Streamlet exception classes."""

from src.streamlet import (
    LoopControlException,
    NodeExecutionException,
    NodeRetryExhaustedException,
    NodeTimeoutException,
    StreamletException,
    UserBusinessException,
    ValidationInputException,
    ValidationOutputException,
)


class TestStreamletException:
    def test_base_exception_defaults(self):
        exc = StreamletException("test message")
        assert str(exc) == "test message"
        assert exc.node_name is None
        assert exc.context == {}
        assert exc.retryable is False

    def test_base_exception_with_node_name(self):
        exc = StreamletException("error", node_name="my_node")
        assert exc.node_name == "my_node"

    def test_base_exception_with_kwargs(self):
        exc = StreamletException("error", extra_data="value")
        assert exc.context == {"extra_data": "value"}


class TestValidationExceptions:
    def test_validation_input_exception(self):
        exc = ValidationInputException("invalid input", node_name="node1")
        assert isinstance(exc, StreamletException)
        assert exc.retryable is False
        assert exc.node_name == "node1"

    def test_validation_input_with_validation_error(self):
        ve = ValueError("type error")
        exc = ValidationInputException("invalid input", validation_error=ve)
        assert exc.validation_error is ve

    def test_validation_output_exception(self):
        exc = ValidationOutputException("invalid output")
        assert isinstance(exc, StreamletException)
        assert exc.retryable is False

    def test_validation_output_with_validation_error(self):
        ve = TypeError("return type error")
        exc = ValidationOutputException("invalid output", validation_error=ve)
        assert exc.validation_error is ve


class TestUserBusinessException:
    def test_default_retryable(self):
        exc = UserBusinessException("business error")
        assert exc.retryable is True

    def test_override_retryable_to_false(self):
        exc = UserBusinessException("business error", retryable=False)
        assert exc.retryable is False

    def test_override_retryable_to_true(self):
        exc = UserBusinessException("business error", retryable=True)
        assert exc.retryable is True

    def test_inherits_from_aetherflow(self):
        exc = UserBusinessException("error")
        assert isinstance(exc, StreamletException)


class TestNodeExecutionException:
    def test_basic(self):
        original = ValueError("original error")
        exc = NodeExecutionException(
            "execution failed", node_name="node1", original_exception=original
        )
        assert exc.node_name == "node1"
        assert exc.original_exception is original

    def test_inheritance_chain(self):
        exc = NodeExecutionException("fail")
        assert isinstance(exc, StreamletException)


class TestNodeTimeoutException:
    def test_basic(self):
        exc = NodeTimeoutException("timeout", node_name="node1", timeout_seconds=30.0)
        assert exc.node_name == "node1"
        assert exc.timeout_seconds == 30.0

    def test_inherits_from_node_execution(self):
        exc = NodeTimeoutException("timeout")
        assert isinstance(exc, NodeExecutionException)


class TestNodeRetryExhaustedException:
    def test_basic(self):
        last_err = RuntimeError("last")
        exc = NodeRetryExhaustedException(
            "retry exhausted", node_name="node1", retry_count=3, last_exception=last_err
        )
        assert exc.node_name == "node1"
        assert exc.retry_count == 3
        assert exc.last_exception is last_err
        assert exc.original_exception is last_err

    def test_inherits_from_node_execution(self):
        exc = NodeRetryExhaustedException("exhausted")
        assert isinstance(exc, NodeExecutionException)


class TestLoopControlException:
    def test_basic(self):
        exc = LoopControlException("loop control")
        assert isinstance(exc, StreamletException)

    def test_retryable_attribute(self):
        exc = LoopControlException("loop")
        assert exc.retryable is False


class TestExceptionRetryablePropagation:
    """Verify retryable attribute is checked correctly."""

    def test_streamlet_exception_not_retryable(self):
        exc = StreamletException("base error")
        from src.streamlet import RetryConfig

        config = RetryConfig()
        assert config.should_retry(exc) is False

    def test_user_business_exception_retryable(self):
        exc = UserBusinessException("business error")
        from src.streamlet import RetryConfig

        config = RetryConfig()
        assert config.should_retry(exc) is True

    def test_user_business_exception_not_retryable_when_set(self):
        exc = UserBusinessException("business error", retryable=False)
        from src.streamlet import RetryConfig

        config = RetryConfig()
        assert config.should_retry(exc) is False

    def test_custom_exception_with_retryable(self):
        class CustomError(Exception):
            retryable = True

        from src.streamlet import RetryConfig

        config = RetryConfig(exception_types=(CustomError,))
        assert config.should_retry(CustomError()) is True
