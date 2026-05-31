"""重试配置与装饰器。"""

import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Callable
from typing import Any

from .exceptions import NodeRetryExhaustedException

logger = logging.getLogger("streamlet")


class RetryConfig:
    """重试配置类"""

    def __init__(
        self,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        exception_types: tuple = (Exception,),
        backoff_factor: float = 1.0,
        max_delay: float = 60.0,
    ) -> None:
        if retry_count < 0:
            raise ValueError(f"retry_count must be >= 0, got {retry_count}")
        if retry_delay < 0:
            raise ValueError(f"retry_delay must be >= 0, got {retry_delay}")
        if max_delay < 0:
            raise ValueError(f"max_delay must be >= 0, got {max_delay}")
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.exception_types = exception_types
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay

    def should_retry(self, exception: Exception) -> bool:
        if hasattr(exception, "retryable"):
            return bool(exception.retryable)
        return isinstance(exception, self.exception_types)

    def get_delay(self, attempt: int) -> float:
        delay = self.retry_delay * (self.backoff_factor**attempt)
        return min(delay, self.max_delay)


def get_func_name(func: Any, fallback_name: str | None = None) -> str:
    """安全获取函数名称"""
    if hasattr(func, "__name__"):
        return str(func.__name__)
    elif hasattr(func, "func") and hasattr(func.func, "__name__"):
        return str(func.func.__name__)
    elif hasattr(func, "name"):
        return str(func.name)
    elif fallback_name:
        return fallback_name
    else:
        return "unknown_function"


def retry_decorator(
    config: RetryConfig,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """重试装饰器"""

    def decorator(func: Callable) -> Callable:
        func_name = node_name or get_func_name(func)

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(config.retry_count + 1):
                    try:
                        logger.debug(
                            f"执行节点 {func_name}，尝试 {attempt + 1}/{config.retry_count + 1}"
                        )
                        result = await func(*args, **kwargs)
                        if attempt > 0:
                            logger.info(
                                f"节点 {func_name} 在第 {attempt + 1} 次尝试后成功"
                            )
                        return result
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as e:
                        if not config.should_retry(e):
                            logger.debug(
                                f"节点 {func_name} 异常不支持重试: {type(e).__name__}: {e}"
                            )
                            raise
                        if attempt == config.retry_count:
                            logger.error(
                                f"节点 {func_name} 重试 {config.retry_count} 次后仍失败: {type(e).__name__}: {e}"
                            )
                            raise NodeRetryExhaustedException(
                                message=f"节点 {func_name} 重试次数耗尽",
                                node_name=func_name,
                                retry_count=config.retry_count,
                                last_exception=e,
                            ) from e
                        delay = config.get_delay(attempt)
                        logger.warning(
                            f"节点 {func_name} 第 {attempt + 1} 次尝试失败: {e}，{delay:.2f}秒后重试"
                        )
                        await asyncio.sleep(delay)

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(config.retry_count + 1):
                    try:
                        logger.debug(
                            f"执行节点 {func_name}，尝试 {attempt + 1}/{config.retry_count + 1}"
                        )
                        result = func(*args, **kwargs)
                        if attempt > 0:
                            logger.info(
                                f"节点 {func_name} 在第 {attempt + 1} 次尝试后成功"
                            )
                        return result
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as e:
                        if not config.should_retry(e):
                            logger.debug(
                                f"节点 {func_name} 异常不支持重试: {type(e).__name__}: {e}"
                            )
                            raise
                        if attempt == config.retry_count:
                            logger.error(
                                f"节点 {func_name} 重试 {config.retry_count} 次后仍失败: {type(e).__name__}: {e}"
                            )
                            raise NodeRetryExhaustedException(
                                message=f"节点 {func_name} 重试次数耗尽",
                                node_name=func_name,
                                retry_count=config.retry_count,
                                last_exception=e,
                            ) from e
                        delay = config.get_delay(attempt)
                        logger.warning(
                            f"节点 {func_name} 第 {attempt + 1} 次尝试失败: {e}，{delay:.2f}秒后重试"
                        )
                        time.sleep(delay)

            return sync_wrapper

    return decorator
