"""Tests for RetryConfig and @node retry integration."""

import functools

import pytest
from pydantic import BaseModel, ValidationError

import streamlet.retry as retry_module
from streamlet import (
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
        "retry_count",
        [
            1.5,
            True,
            "3",
        ],
    )
    def test_invalid_retry_count_type_raises(self, retry_count):
        with pytest.raises(TypeError, match="retry_count"):
            RetryConfig(retry_count=retry_count)

    @pytest.mark.parametrize(
        ("field_name", "value"),
        [
            ("retry_delay", True),
            ("retry_delay", "1.0"),
            ("backoff_factor", False),
            ("backoff_factor", "2.0"),
            ("max_delay", True),
            ("max_delay", "60.0"),
        ],
    )
    def test_invalid_numeric_type_raises(self, field_name, value):
        with pytest.raises(TypeError, match=field_name):
            RetryConfig(**{field_name: value})

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


class TestGetFuncName:
    def test_uses_wrapped_func_name_for_partial(self):
        def original(value: int) -> int:
            return value

        partial_func = functools.partial(original, 1)

        assert retry_module.get_func_name(partial_func) == "original"

    def test_uses_name_attribute_when_callable_has_no_dunder_name(self):
        class NamedCallable:
            name = "configured_name"

            def __call__(self) -> None:
                pass

        assert retry_module.get_func_name(NamedCallable()) == "configured_name"

    def test_uses_explicit_fallback_name(self):
        unnamed = object()

        assert retry_module.get_func_name(unnamed, "fallback") == "fallback"

    def test_uses_unknown_function_when_no_name_is_available(self):
        unnamed = object()

        assert retry_module.get_func_name(unnamed) == "unknown_function"


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

    def test_node_non_retryable_exception_is_not_retried(self):
        call_count = 0

        class TempError(Exception):
            retryable = False

        @node(
            retry_count=3,
            retry_delay=0,
            exception_types=(TempError,),
            enable_retry=True,
        )
        def flaky_node() -> None:
            nonlocal call_count
            call_count += 1
            raise TempError("do not retry")

        with pytest.raises(TempError, match="do not retry"):
            flaky_node()

        assert call_count == 1

    def test_node_keyboard_interrupt_is_not_wrapped_or_retried(self):
        call_count = 0

        @node(retry_count=3, retry_delay=0, enable_retry=True)
        def interrupted_node() -> None:
            nonlocal call_count
            call_count += 1
            raise KeyboardInterrupt("stop now")

        with pytest.raises(KeyboardInterrupt, match="stop now"):
            interrupted_node()

        assert call_count == 1

    def test_node_retries_function_body_pydantic_validation_error(self):
        call_count = 0

        class Payload(BaseModel):
            x: int

        @node(retry_count=1, retry_delay=0, enable_retry=True)
        def build_payload() -> None:
            nonlocal call_count
            call_count += 1
            Payload(x="bad")

        with pytest.raises(NodeRetryExhaustedException) as exc_info:
            build_payload()
        assert call_count == 2
        assert isinstance(exc_info.value.last_exception, ValidationError)

    def test_node_without_retry(self):
        @node(enable_retry=False)
        def stable_node(x: int) -> int:
            return x * 2

        result = stable_node(5)
        assert result == 10

    def test_node_ignores_retry_arguments_when_retry_disabled(self):
        @node(retry_count=-1, retry_delay=-0.1, enable_retry=False)
        def stable_node(x: int) -> int:
            return x * 2

        assert stable_node(5) == 10

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

    @pytest.mark.asyncio
    async def test_async_node_awaits_retry_delay_from_config(self, monkeypatch):
        call_count = 0
        sleep_delays = []

        class TempError(Exception):
            retryable = True

        async def fake_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        monkeypatch.setattr(retry_module.asyncio, "sleep", fake_sleep)

        @node(
            retry_count=3,
            retry_delay=0.25,
            exception_types=(TempError,),
            backoff_factor=2.0,
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
        assert sleep_delays == [0.25, 0.5]

    @pytest.mark.asyncio
    async def test_async_node_retry_exhausted(self):
        call_count = 0

        class TempError(Exception):
            retryable = True

        @node(
            retry_count=1,
            retry_delay=0,
            exception_types=(TempError,),
            enable_retry=True,
        )
        async def flaky_async(x: int) -> int:
            nonlocal call_count
            call_count += 1
            raise TempError(f"always fails for {x}")

        with pytest.raises(NodeRetryExhaustedException) as exc_info:
            await flaky_async(5)

        assert call_count == 2
        assert exc_info.value.node_name == "flaky_async"
        assert exc_info.value.retry_count == 1
        assert isinstance(exc_info.value.last_exception, TempError)
        assert exc_info.value.original_exception is exc_info.value.last_exception
        assert exc_info.value.__cause__ is exc_info.value.last_exception

    @pytest.mark.asyncio
    async def test_async_node_non_retryable_exception_is_not_retried(self):
        call_count = 0

        class TempError(Exception):
            retryable = False

        @node(
            retry_count=3,
            retry_delay=0,
            exception_types=(TempError,),
            enable_retry=True,
        )
        async def flaky_async() -> None:
            nonlocal call_count
            call_count += 1
            raise TempError("do not retry")

        with pytest.raises(TempError, match="do not retry"):
            await flaky_async()

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_async_node_exception_type_mismatch_is_not_retried(self):
        call_count = 0

        @node(
            retry_count=3,
            retry_delay=0,
            exception_types=(ValueError,),
            enable_retry=True,
        )
        async def flaky_async() -> None:
            nonlocal call_count
            call_count += 1
            raise TypeError("wrong exception type")

        with pytest.raises(TypeError, match="wrong exception type"):
            await flaky_async()

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_async_node_keyboard_interrupt_is_not_wrapped_or_retried(self):
        call_count = 0

        @node(retry_count=3, retry_delay=0, enable_retry=True)
        async def interrupted_async() -> None:
            nonlocal call_count
            call_count += 1
            raise KeyboardInterrupt("stop now")

        with pytest.raises(KeyboardInterrupt, match="stop now"):
            await interrupted_async()

        assert call_count == 1
