"""异常类型体系。"""

from typing import Any


class StreamletException(Exception):
    """Streamlet 框架基础异常类"""

    retryable = False

    def __init__(
        self, message: str, node_name: str | None = None, **kwargs: Any
    ) -> None:
        self.node_name = node_name
        self.context = kwargs
        super().__init__(message)


class ValidationInputException(StreamletException):
    """参数验证异常——validate_call 前置校验失败"""

    retryable = False

    def __init__(
        self,
        message: str,
        validation_error: Any = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.validation_error = validation_error
        super().__init__(message, node_name, **kwargs)


class ValidationOutputException(StreamletException):
    """返回值验证异常——validate_call 返回值校验失败"""

    retryable = False

    def __init__(
        self,
        message: str,
        validation_error: Any = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.validation_error = validation_error
        super().__init__(message, node_name, **kwargs)


class UserBusinessException(StreamletException):
    """用户业务异常基类——用户可自定义重试策略"""

    retryable = True

    def __init__(
        self,
        message: str,
        retryable: bool | None = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        if retryable is not None:
            self.retryable = retryable
        super().__init__(message, node_name, **kwargs)


class NodeExecutionException(StreamletException):
    """节点执行异常"""

    def __init__(
        self,
        message: str,
        node_name: str | None = None,
        original_exception: Exception | None = None,
        **kwargs: Any,
    ) -> None:
        self.original_exception = original_exception
        super().__init__(message, node_name, **kwargs)


class NodeTimeoutException(NodeExecutionException):
    """节点执行超时异常"""

    def __init__(
        self,
        message: str,
        node_name: str | None = None,
        timeout_seconds: float | None = None,
        **kwargs: Any,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(message, node_name, **kwargs)


class NodeRetryExhaustedException(NodeExecutionException):
    """节点重试次数耗尽异常"""

    def __init__(
        self,
        message: str,
        node_name: str | None = None,
        retry_count: int | None = None,
        last_exception: Exception | None = None,
        **kwargs: Any,
    ) -> None:
        self.retry_count = retry_count
        self.last_exception = last_exception
        super().__init__(
            message, node_name, original_exception=last_exception, **kwargs
        )


class LoopControlException(StreamletException):
    """循环控制异常基类"""

    pass
