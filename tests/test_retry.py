"""Tests for RetryConfig and @node retry integration."""

import pytest

from src.streamlet import (
    NodeRetryExhaustedException,
    RetryConfig,
    UserBusinessException,
    node,
)


class TestRetryConfig:
    def test_default_values(self):
        config = RetryConfig()
        assert config.retry_count == 3
        assert config.retry_delay == 1.0
        assert config.exception_types == (Exception,)
        assert config.backoff_factor == 1.0
        assert config.max_delay == 60.0

    def test_custom_values(self):
        config = RetryConfig(
            retry_count=5, retry_delay=0.5, backoff_factor=2.0, max_delay=10.0
        )
        assert config.retry_count == 5
        assert config.retry_delay == 0.5
        assert config.backoff_factor == 2.0
        assert config.max_delay == 10.0

    def test_get_delay_no_backoff(self):
        config = RetryConfig(retry_delay=1.0, backoff_factor=1.0)
        assert config.get_delay(0) == 1.0
        assert config.get_delay(3) == 1.0

    def test_get_delay_with_backoff(self):
        config = RetryConfig(retry_delay=1.0, backoff_factor=2.0)
        assert config.get_delay(0) == 1.0
        assert config.get_delay(1) == 2.0
        assert config.get_delay(2) == 4.0

    def test_get_delay_capped_by_max(self):
        config = RetryConfig(retry_delay=1.0, backoff_factor=10.0, max_delay=5.0)
        assert config.get_delay(3) == 5.0  # 1.0 * 10^3 = 1000, capped at 5.0

    def test_should_retry_by_exception_type(self):
        config = RetryConfig(exception_types=(ValueError,))
        assert config.should_retry(ValueError("x")) is True
        assert config.should_retry(TypeError("x")) is False

    def test_should_retry_by_retryable_attribute(self):
        config = RetryConfig()

        class RetryableError(Exception):
            retryable = True

        class NonRetryableError(Exception):
            retryable = False

        assert config.should_retry(RetryableError()) is True
        assert config.should_retry(NonRetryableError()) is False

    def test_should_retry_user_business_exception(self):
        config = RetryConfig()
        assert config.should_retry(UserBusinessException("error")) is True
        assert (
            config.should_retry(UserBusinessException("error", retryable=False))
            is False
        )

    def test_negative_retry_count_raises(self):
        with pytest.raises(ValueError, match="retry_count"):
            RetryConfig(retry_count=-1)

    def test_negative_retry_delay_raises(self):
        with pytest.raises(ValueError, match="retry_delay"):
            RetryConfig(retry_delay=-0.1)

    def test_negative_backoff_factor_raises(self):
        with pytest.raises(ValueError, match="backoff_factor"):
            RetryConfig(backoff_factor=-1.0)

    def test_negative_max_delay_raises(self):
        with pytest.raises(ValueError, match="max_delay"):
            RetryConfig(max_delay=-1.0)

    @pytest.mark.parametrize(
        "exception_types",
        [
            ValueError,
            (ValueError, "not_exception"),
            (ValueError, 42),
        ],
    )
    def test_invalid_exception_types_raise(self, exception_types):
        with pytest.raises(TypeError, match="exception_types"):
            RetryConfig(exception_types=exception_types)


class TestNodeWithRetry:
    def test_node_with_enable_retry_succeeds(self):
        call_count = 0

        class TempError(Exception):
            retryable = True

        @node(
            retry_count=3,
            retry_delay=0.01,
            exception_types=(TempError,),
            enable_retry=True,
        )
        def flaky_node(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TempError("retry")
            return x * 2

        result = flaky_node(5)
        assert result == 10
        assert call_count == 3

    def test_node_retry_exhausted(self):
        class TempError(Exception):
            retryable = True

        @node(
            retry_count=1,
            retry_delay=0.01,
            exception_types=(TempError,),
            enable_retry=True,
        )
        def flaky_node(x: int) -> int:
            raise TempError("always fails")

        with pytest.raises(NodeRetryExhaustedException) as exc_info:
            flaky_node(5)
        assert exc_info.value.node_name == "flaky_node"
        assert exc_info.value.retry_count == 1
        assert isinstance(exc_info.value.last_exception, TempError)

    def test_node_without_retry(self):
        @node(enable_retry=False)
        def stable_node(x: int) -> int:
            return x * 2

        result = stable_node(5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_async_node_with_retry(self):
        call_count = 0

        class TempError(Exception):
            retryable = True

        @node(
            retry_count=3,
            retry_delay=0.01,
            exception_types=(TempError,),
            enable_retry=True,
        )
        async def flaky_async(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TempError("retry")
            return x * 2

        result = await flaky_async(5)
        assert result == 10
        assert call_count == 3
