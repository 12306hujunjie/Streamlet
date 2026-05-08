import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from dependency_injector import containers, providers
from dependency_injector.wiring import inject
from pydantic import ConfigDict, TypeAdapter, ValidationError, validate_call

logger = logging.getLogger("aetherflow")


@dataclass
class ParallelResult:
    """Pydantic model for recording parallel execution results and exception stacks."""

    node_name: str
    success: bool
    result: Any = None
    error: str | None = None
    error_traceback: str | None = None
    execution_time: float | None = None


# ==================== 异常类型体系 ====================
class AetherFlowException(Exception):
    """AetherFlow框架基础异常类"""

    retryable = False  # 默认框架异常不重试

    def __init__(
        self, message: str, node_name: str | None = None, **kwargs: Any
    ) -> None:
        self.node_name = node_name
        self.context = kwargs
        super().__init__(message)


class ValidationInputException(AetherFlowException):
    """参数验证异常 - validate_call前置校验失败"""

    retryable = False  # 参数验证失败不应该重试

    def __init__(
        self,
        message: str,
        validation_error: Any = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.validation_error = validation_error
        super().__init__(message, node_name, **kwargs)


class ValidationOutputException(AetherFlowException):
    """返回值验证异常 - validate_call返回值校验失败"""

    retryable = False  # 返回值验证失败不应该重试

    def __init__(
        self,
        message: str,
        validation_error: Any = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.validation_error = validation_error
        super().__init__(message, node_name, **kwargs)


class UserBusinessException(AetherFlowException):
    """用户业务异常基类 - 用户可自定义重试策略"""

    retryable = True  # 默认用户业务异常可重试

    def __init__(
        self,
        message: str,
        retryable: bool | None = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        # 允许用户在实例化时覆盖重试策略
        if retryable is not None:
            self.retryable = retryable
        super().__init__(message, node_name, **kwargs)


class NodeExecutionException(AetherFlowException):
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


class LoopControlException(AetherFlowException):
    """循环控制异常基类"""

    pass


# ==================== 重试装饰器 ====================


class RetryConfig:
    """重试配置类"""

    def __init__(
        self,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        exception_types: tuple = (Exception,),
        backoff_factor: float = 1.0,
        max_delay: float = 60.0,
    ):
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
        """判断是否应该重试 - 优先检查retryable属性，否则使用isinstance检查继承关系"""
        # 如果异常有retryable属性，优先使用
        if hasattr(exception, "retryable"):
            return bool(exception.retryable)

        # 否则使用isinstance检查异常是否属于指定类型（包括继承关系）
        return isinstance(exception, self.exception_types)

    def get_delay(self, attempt: int) -> float:
        """计算重试延迟时间（支持指数退避）"""
        delay = self.retry_delay * (self.backoff_factor**attempt)
        return min(delay, self.max_delay)


def _get_func_name(func: Any, fallback_name: str | None = None) -> str:
    """安全获取函数名称"""
    if hasattr(func, "__name__"):
        return str(func.__name__)
    elif hasattr(func, "func") and hasattr(func.func, "__name__"):  # partial对象
        return str(func.func.__name__)
    elif hasattr(func, "name"):  # Node对象
        return str(func.name)
    elif fallback_name:
        return fallback_name
    else:
        return "unknown_function"


def retry_decorator(
    config: RetryConfig,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """重试装饰器

    Args:
        config: RetryConfig 配置模型
        node_name: 节点名称，用于异常信息
    """

    def decorator(func: Callable) -> Callable:
        func_name = node_name or _get_func_name(func)

        if inspect.iscoroutinefunction(func):
            # 异步函数wrapper
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(config.retry_count + 1):
                    try:
                        logger.debug(
                            f"执行节点 {func_name}，尝试 {attempt + 1}/{config.retry_count + 1}"
                        )
                        result = await func(*args, **kwargs)  # 异步调用

                        if attempt > 0:
                            logger.info(
                                f"节点 {func_name} 在第 {attempt + 1} 次尝试后成功"
                            )
                        return result

                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as e:
                        if not config.should_retry(e):
                            # 记录不重试的原因
                            logger.debug(
                                f"节点 {func_name} 异常不支持重试: {type(e).__name__}: {e}"
                            )
                            raise  # 直接抛出，不封装

                        if attempt == config.retry_count:
                            # 记录重试耗尽
                            logger.error(
                                f"节点 {func_name} 重试 {config.retry_count} 次后仍失败: {type(e).__name__}: {e}"
                            )
                            raise  # 重试耗尽也直接抛出，不封装

                        delay = config.get_delay(attempt)
                        logger.warning(
                            f"节点 {func_name} 第 {attempt + 1} 次尝试失败: {e}，{delay:.2f}秒后重试"
                        )
                        await asyncio.sleep(delay)  # 异步延迟

            return async_wrapper
        else:
            # 同步函数wrapper
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(config.retry_count + 1):
                    try:
                        logger.debug(
                            f"执行节点 {func_name}，尝试 {attempt + 1}/{config.retry_count + 1}"
                        )
                        result = func(*args, **kwargs)  # 同步调用

                        if attempt > 0:
                            logger.info(
                                f"节点 {func_name} 在第 {attempt + 1} 次尝试后成功"
                            )
                        return result

                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as e:
                        if not config.should_retry(e):
                            # 记录不重试的原因
                            logger.debug(
                                f"节点 {func_name} 异常不支持重试: {type(e).__name__}: {e}"
                            )
                            raise  # 直接抛出，不封装

                        if attempt == config.retry_count:
                            # 记录重试耗尽
                            logger.error(
                                f"节点 {func_name} 重试 {config.retry_count} 次后仍失败: {type(e).__name__}: {e}"
                            )
                            raise  # 重试耗尽也直接抛出，不封装

                        delay = config.get_delay(attempt)
                        logger.warning(
                            f"节点 {func_name} 第 {attempt + 1} 次尝试失败: {e}，{delay:.2f}秒后重试"
                        )
                        time.sleep(delay)  # 同步延迟

            return sync_wrapper

    return decorator


# Context variables for asyncio coroutine safety
_context_state: ContextVar[dict | None] = ContextVar("aetherflow_state", default=None)
_context_context: ContextVar[dict | None] = ContextVar(
    "aetherflow_context", default=None
)


# ==================== 自定义ContextVar Provider ====================


class ContextVarProvider(providers.Provider):
    """自定义Provider类，支持ContextVar的协程安全依赖注入。

    这个Provider替代了直接调用ContextVar.get()的方式，
    提供了正确的dependency-injector集成。
    """

    def __init__(self, default_factory: Callable[[], Any] = dict):
        """初始化ContextVarProvider。

        Args:
            default_factory: 创建默认值的工厂函数，默认为dict
        """
        super().__init__()
        self._context_var = ContextVar(f"aetherflow_{id(self)}", default=None)
        self._default_factory = default_factory

    def _provide(self, *args: Any, **kwargs: Any) -> Any:
        """提供协程安全的状态值。

        Returns:
            ContextVar中的值，如果未设置则返回默认值
        """
        try:
            value = self._context_var.get()
            if value is None:
                # 如果未设置，创建并设置默认值
                value = self._default_factory()
                self._context_var.set(value)
            return value
        except LookupError:
            # 如果ContextVar未初始化，创建默认值
            value = self._default_factory()
            self._context_var.set(value)
            return value


class BaseFlowContext(containers.DeclarativeContainer):
    """Base container for flow context with thread-safe and coroutine-safe dependency injection support."""

    # Use ThreadLocalSingleton for thread-local state isolation
    # Each thread gets its own state dictionary
    state: providers.Provider = providers.ThreadLocalSingleton(dict)
    context: providers.Provider = providers.ThreadLocalSingleton(dict)
    shared_data: providers.Provider = providers.Singleton(dict)

    # Coroutine-safe providers using ContextVar for asyncio
    async_state: providers.Provider = ContextVarProvider(dict)
    async_context: providers.Provider = ContextVarProvider(dict)


def custom_validate_call(
    validate_return: bool = True,
    config: ConfigDict | None = None,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    自定义validate_call包装器，使用Pydantic最佳实践区分输入验证和输出验证异常

    Args:
        validate_return: 是否验证返回值
        config: Pydantic配置
        node_name: 节点名称用于异常信息

    Returns:
        装饰器函数
    """

    def decorator(func: Callable) -> Callable:
        # 获取函数签名
        sig = inspect.signature(func)

        # 创建输入验证器 - 只验证参数，不验证返回值
        input_validator = validate_call(
            validate_return=False,
            config=config or ConfigDict(arbitrary_types_allowed=True),
        )(func)

        # 创建返回值验证器（如果需要且有返回值类型注解）
        return_type_adapter = None
        if validate_return and sig.return_annotation != inspect.Signature.empty:
            return_type_adapter = TypeAdapter(sig.return_annotation)

        # 提取公共逻辑
        func_name = node_name or _get_func_name(func)

        def create_input_exception(e: ValidationError) -> ValidationInputException:
            return ValidationInputException(
                f"输入参数验证失败: {e}",
                validation_error=e,
                node_name=func_name,
            )

        def create_output_exception(e: ValidationError) -> ValidationOutputException:
            return ValidationOutputException(
                f"返回值验证失败: {e}",
                validation_error=e,
                node_name=func_name,
            )

        def validate_result(result: Any) -> Any:
            if return_type_adapter:
                try:
                    return_type_adapter.validate_python(result)
                except ValidationError as e:
                    raise create_output_exception(e) from e
            return result

        # 根据函数类型提供对应的wrapper
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    result = await input_validator(*args, **kwargs)
                except ValidationError as e:
                    raise create_input_exception(e) from e
                return validate_result(result)

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    result = input_validator(*args, **kwargs)
                except ValidationError as e:
                    raise create_input_exception(e) from e
                return validate_result(result)

            return sync_wrapper

    return decorator


class Node:
    """A node in the execution graph that supports fluent interface methods."""

    def __init__(
        self,
        func: Callable,
        name: str,
        is_start_node: bool = True,
        is_async: bool | None = None,
    ):
        # 配置Pydantic支持任意类型（包括dependency injection的类型）
        self.func = func
        self.name = name
        self.is_start_node = is_start_node
        # 智能检测异步特性：处理Node对象、装饰器等复杂情况
        if is_async is not None:
            # 显式传入，直接使用
            self.is_async = is_async
        elif isinstance(func, Node):
            # func是Node对象，使用其is_async属性
            self.is_async = func.is_async
        else:
            # func是普通函数，使用inspect检测
            self.is_async = inspect.iscoroutinefunction(func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """智能异步适配调用：根据函数类型和执行上下文自动处理同步/异步调用。"""
        if self.is_async:
            # 异步函数：根据当前执行上下文智能处理
            try:
                # 检查是否在事件循环中
                asyncio.get_running_loop()
                # 在事件循环中，返回协程对象让调用者await
                return self.func(*args, **kwargs)
            except RuntimeError:
                # 不在事件循环中，创建新事件循环同步执行
                return asyncio.run(self.func(*args, **kwargs))
        else:
            # 同步函数：直接执行
            return self.func(*args, **kwargs)

    def then(self, next_node: "Node") -> "Node":
        """Chain this node with another node for sequential execution."""
        return sequential_composition(self, next_node)

    def fan_out_to(
        self,
        nodes: list["Node"],
        executor: str = "thread",
        max_workers: int | None = None,
    ) -> "Node":
        """Fan out to multiple nodes for parallel execution."""
        # Normalize executor type to lowercase for case-insensitive comparison
        executor_lower = executor.lower()
        if executor_lower not in ["thread", "async", "auto"]:
            raise ValueError(
                "Only 'thread', 'async', and 'auto' executors are supported. ProcessPoolExecutor has been removed to resolve pickle serialization issues."
            )
        return parallel_fan_out(self, nodes, executor_lower, max_workers)

    def fan_in(self, aggregator: "Node") -> "Node":
        """Aggregate results using the specified aggregator node."""
        return parallel_fan_in(self, aggregator)

    def fan_out_in(
        self,
        targets: list["Node"],
        aggregator: "Node",
        executor: str = "thread",
        max_workers: int | None = None,
    ) -> "Node":
        """Complete fan-out and fan-in operation in one step."""
        # Normalize executor type to lowercase for case-insensitive comparison
        executor_lower = executor.lower()
        if executor_lower not in ["thread", "async", "auto"]:
            raise ValueError(
                "Only 'thread', 'async', and 'auto' executors are supported. ProcessPoolExecutor has been removed to resolve pickle serialization issues."
            )
        return parallel_fan_out_in(
            self, targets, aggregator, executor_lower, max_workers
        )

    def branch_on(self, conditions: dict[bool, "Node"]) -> "Node":
        """Branch execution based on the boolean output of this node."""
        return conditional_composition(self, conditions)

    def repeat(self, times: int, stop_on_error: bool = False) -> "Node":
        """重复执行此节点。

        Args:
            times: 重复次数
            stop_on_error: 遇到错误时是否立即停止
        """
        return repeat_composition(self, times, stop_on_error)

    def __repr__(self) -> str:
        return f"Node(name='{self.name}')"


def sequential_composition(left: Node, right: Node) -> Node:
    """Sequential execution that combines two nodes with one-time type inference."""

    # 组合时一次性检测是否有异步节点
    has_async = left.is_async or right.is_async

    composition_name = f"({left.name} -> {right.name})"

    if has_async:
        # 如果包含异步节点，创建异步组合函数
        async def async_run(*args: Any, **kwargs: Any) -> Any:
            # 执行左节点，使用Node.__call__智能适配
            left_result = (
                await left(*args, **kwargs) if left.is_async else left(*args, **kwargs)
            )

            # 执行右节点，使用Node.__call__智能适配
            right_result = (
                await right(left_result) if right.is_async else right(left_result)
            )

            return right_result

        return Node(func=async_run, name=composition_name, is_start_node=False)
    else:
        # 如果都是同步节点，创建同步组合函数
        def run(*args: Any, **kwargs: Any) -> Any:
            left_result = left(*args, **kwargs)
            right_result = right(left_result)
            return right_result

        return Node(func=run, name=composition_name, is_start_node=False)


def _generate_unique_result_key(base_name: str, existing_results: dict) -> str:
    """生成唯一的结果键，避免重复覆盖

    Args:
        base_name: 基础键名（通常是节点名称）
        existing_results: 现有的结果字典

    Returns:
        唯一的键名，如果base_name无冲突则返回原名，否则返回带数字后缀的名称
    """
    if base_name not in existing_results:
        return base_name

    counter = 1
    unique_key = f"{base_name}[{counter}]"
    while unique_key in existing_results:
        counter += 1
        unique_key = f"{base_name}[{counter}]"

    return unique_key


# 定义并行任务执行函数
def execute_target_node(node: Node, input_data: Any) -> ParallelResult:
    """Execute a single target node with the provided input using intelligent async/sync handling."""
    import traceback

    start_time = time.time()

    try:
        # 使用Node.__call__智能异步适配
        result = node(input_data)
        execution_time = time.time() - start_time

        return ParallelResult(
            node_name=node.name,
            success=True,
            result=result,
            execution_time=execution_time,
        )
    except Exception as e:
        execution_time = time.time() - start_time
        error_traceback = traceback.format_exc()
        logger.error(f"Node '{node.name}' failed: {e}")

        return ParallelResult(
            node_name=node.name,
            success=False,
            error=str(e),
            error_traceback=error_traceback,
            execution_time=execution_time,
        )


def parallel_fan_out(
    source: Node,
    targets: list[Node],
    executor: str = "thread",
    max_workers: int | None = None,
) -> Node:
    """
    Simplified parallel fan-out execution with type-based executor selection.

    Args:
        source: Source node to execute first
        targets: List of target nodes for parallel execution
        executor: 'thread', 'async', or 'auto' for automatic selection
        max_workers: Maximum worker threads (ignored for async)

    Returns:
        Node that performs parallel fan-out execution
    """
    if not targets:
        raise ValueError("Target nodes list cannot be empty")

    executor = executor.lower()

    # Simplified 'auto' executor selection based on one-time type inference
    if executor == "auto":
        all_nodes = [source] + targets
        has_async = any(node.is_async for node in all_nodes)
        executor = "async" if has_async else "thread"
        logger.info(f"Auto-selected executor '{executor}' based on node types")

    if executor not in ["thread", "async"]:
        raise ValueError("Only 'thread', 'async', and 'auto' executors are supported.")

    target_names = [t.name for t in targets]
    composition_name = f"({source.name} -> [{', '.join(target_names)}])"

    if executor == "async":
        # Simplified async version using Node.__call__ smart adaptation
        async def run_async(*args: Any, **kwargs: Any) -> dict[str, ParallelResult]:
            logger.info(f"Executing Async Parallel Fan-Out: {composition_name}")

            # Execute source node with consistent async handling
            source_result = (
                await source(*args, **kwargs)
                if source.is_async
                else source(*args, **kwargs)
            )

            # Execute target nodes in parallel
            async def execute_async_target(
                node: Node, input_data: Any
            ) -> ParallelResult:
                start_time = time.time()
                try:
                    # Consistent async handling with sequential_composition
                    result = (
                        await node(input_data) if node.is_async else node(input_data)
                    )

                    execution_time = time.time() - start_time
                    return ParallelResult(
                        node_name=node.name,
                        success=True,
                        result=result,
                        execution_time=execution_time,
                    )
                except Exception as e:
                    import traceback

                    execution_time = time.time() - start_time
                    return ParallelResult(
                        node_name=node.name,
                        success=False,
                        error=str(e),
                        error_traceback=traceback.format_exc(),
                        execution_time=execution_time,
                    )

            # Create and execute tasks
            tasks = [execute_async_target(node, source_result) for node in targets]
            results = await asyncio.gather(*tasks)

            # Collect results with unique keys
            parallel_results: dict[str, ParallelResult] = {}
            for result in results:
                result_key = _generate_unique_result_key(
                    result.node_name, parallel_results
                )
                parallel_results[result_key] = result

            logger.info(
                f"Async parallel fan-out completed with {len(parallel_results)} results"
            )
            return parallel_results

        return Node(func=run_async, name=composition_name)

    else:
        # Simplified thread version using Node.__call__ smart adaptation
        def run_thread(*args: Any, **kwargs: Any) -> dict[str, ParallelResult]:
            logger.info(f"Executing Thread Parallel Fan-Out: {composition_name}")

            # Execute source node with smart adaptation
            source_result = source(*args, **kwargs)

            # Execute target nodes in parallel using ThreadPoolExecutor
            parallel_results: dict[str, ParallelResult] = {}

            with ThreadPoolExecutor(max_workers=max_workers) as executor_instance:
                # Submit all parallel tasks
                future_to_node = {
                    executor_instance.submit(
                        execute_target_node, node, source_result
                    ): node
                    for node in targets
                }

                # Collect results
                for future in as_completed(future_to_node):
                    node = future_to_node[future]
                    try:
                        parallel_result = future.result()

                        # Generate unique result key
                        result_key = _generate_unique_result_key(
                            parallel_result.node_name, parallel_results
                        )

                        parallel_results[result_key] = parallel_result
                        logger.debug(
                            f"Collected result from '{result_key}': success={parallel_result.success}"
                        )

                    except Exception as e:
                        import traceback

                        logger.error(f"Failed to get result from '{node.name}': {e}")

                        # Generate unique result key to avoid key overwriting in exception cases
                        error_result_key = _generate_unique_result_key(
                            node.name, parallel_results
                        )
                        parallel_results[error_result_key] = ParallelResult(
                            node_name=node.name,
                            success=False,
                            error=str(e),
                            error_traceback=traceback.format_exc(),
                        )

            # Return parallel results
            logger.info(
                f"Thread parallel fan-out completed with {len(parallel_results)} results"
            )
            return parallel_results

        return Node(func=run_thread, name=composition_name)


def parallel_fan_in(fan_out_node: Node, aggregator: Node) -> Node:
    """
    Simplified parallel fan-in aggregation with intelligent async/sync handling.

    Args:
        fan_out_node: The fan-out node to execute first
        aggregator: The aggregator node that combines results

    Returns:
        Node that performs fan-in aggregation
    """

    # 检测是否包含异步节点，与sequential_composition保持一致
    has_async = fan_out_node.is_async or aggregator.is_async

    composition_name = f"({fan_out_node.name} -> {aggregator.name})"

    if has_async:
        # 包含异步节点，创建异步组合函数
        async def async_run(*args: Any, **kwargs: Any) -> Any:
            logger.info(f"Executing Parallel Fan-In (Async): {composition_name}")

            # 执行fan-out节点，智能适配异步/同步
            fan_out_result = (
                await fan_out_node(*args, **kwargs)
                if fan_out_node.is_async
                else fan_out_node(*args, **kwargs)
            )

            # 执行聚合器，智能适配异步/同步
            aggregator_result = (
                await aggregator(fan_out_result)
                if aggregator.is_async
                else aggregator(fan_out_result)
            )

            logger.info("Fan-in aggregation completed successfully")
            return aggregator_result

        return Node(func=async_run, name=composition_name, is_start_node=False)
    else:
        # 都是同步节点，创建同步组合函数
        def run(*args: Any, **kwargs: Any) -> Any:
            logger.info(f"Executing Parallel Fan-In (Sync): {composition_name}")

            # 执行fan-out节点，获取并行结果
            parallel_results = fan_out_node(*args, **kwargs)

            # 将并行结果作为参数传递给聚合器
            aggregator_result = aggregator(parallel_results)

            logger.info("Fan-in aggregation completed successfully")
            return aggregator_result

        return Node(func=run, name=composition_name, is_start_node=False)


def parallel_fan_out_in(
    source: Node,
    targets: list[Node],
    aggregator: Node,
    executor: str = "thread",
    max_workers: int | None = None,
) -> Node:
    """
    Convenience function that combines fan-out and fan-in into a single operation.

    Args:
        source: Source node to execute first
        targets: List of target nodes for parallel execution
        aggregator: Aggregator node that combines parallel results
        executor: 'thread', 'async', or 'auto' for automatic selection
        max_workers: Maximum worker threads (ignored for async)

    Returns:
        Node that performs complete fan-out-in operation
    """
    # Normalize executor type to lowercase for case-insensitive comparison
    executor = executor.lower()

    # Handle 'auto' executor selection using simplified one-time type inference
    if executor == "auto":
        # Analyze all nodes (source + targets + aggregator) for optimal executor choice
        all_nodes = [source] + targets + [aggregator]
        has_async = any(node.is_async for node in all_nodes)
        executor = "async" if has_async else "thread"
        logger.info(
            f"Auto-selected executor '{executor}' based on node types for fan-out-in"
        )

    if executor not in ["thread", "async"]:
        raise ValueError(
            "Only 'thread', 'async', and 'auto' executors are supported. ProcessPoolExecutor has been removed to resolve pickle serialization issues."
        )
    # 创建fan-out节点
    fan_out_node = parallel_fan_out(
        source=source, targets=targets, executor=executor, max_workers=max_workers
    )

    # 创建fan-in节点
    return parallel_fan_in(fan_out_node, aggregator)


def conditional_composition(condition_node: Node, branches: dict[Any, Node]) -> Node:
    """Conditional branching based on boolean output."""

    # 检测是否有异步节点
    has_async = condition_node.is_async or any(
        branch.is_async for branch in branches.values()
    )

    branch_names = {k: v.name for k, v in branches.items()}
    composition_name = f"({condition_node.name} ? {branch_names})"

    if has_async:
        # 异步版本
        async def async_run(*args: Any, **kwargs: Any) -> Any:
            logger.info(f"--- Executing Conditional Branch: {composition_name} ---")

            # 执行条件节点
            condition_result = (
                await condition_node(*args, **kwargs)
                if condition_node.is_async
                else condition_node(*args, **kwargs)
            )

            # 执行对应的分支
            if condition_result in branches:
                selected_branch = branches[condition_result]
                logger.info(
                    f"Condition is {condition_result}, executing branch: {selected_branch.name}"
                )
                return (
                    await selected_branch()
                    if selected_branch.is_async
                    else selected_branch()
                )
            else:
                msg = f"No branch defined for condition result: {condition_result}"
                logger.error(msg)
                raise ValueError(msg)

        return Node(
            func=async_run,
            name=composition_name,
        )
    else:
        # 同步版本
        def run(*args: Any, **kwargs: Any) -> Any:
            logger.info(f"--- Executing Conditional Branch: {composition_name} ---")

            # Execute condition node
            condition_result = condition_node(*args, **kwargs)

            # Execute the appropriate branch
            if condition_result in branches:
                selected_branch = branches[condition_result]
                logger.info(
                    f"Condition is {condition_result}, executing branch: {selected_branch.name}"
                )
                return selected_branch()
            else:
                msg = f"No branch defined for condition result: {condition_result}"
                logger.error(msg)
                raise ValueError(msg)

        return Node(
            func=run,
            name=composition_name,
        )


def repeat_composition(node: Node, times: int, stop_on_error: bool = False) -> Node:
    """重复执行节点的简化版本。

    Args:
        node: 要重复执行的节点
        times: 重复次数
        stop_on_error: 遇到错误时是否立即停止

    Returns:
        包装后的重复执行节点
    """

    # 参数前置验证：立即检查参数，fail-fast原则
    if not isinstance(times, int):
        raise TypeError(f"times must be an integer, got {type(times).__name__}")
    if times <= 0:
        raise ValueError("Repeat times must be greater than 0")
    if not isinstance(node, Node):
        raise TypeError("node must be a Node instance")

    # 创建组合节点的名称
    composition_name = f"({node.name} * {times})"

    # 检测是否包含异步节点，决定创建同步还是异步执行函数
    if node.is_async:
        # 异步节点：创建异步执行函数
        async def async_run(*args: Any, **kwargs: Any) -> Any:
            logger.info(
                f"--- Executing Async Repeat Composition: {composition_name} ---"
            )

            last_result = None
            errors = []

            for i in range(times):
                logger.info(f"  - Iteration {i + 1}/{times}")

                try:
                    # 正常执行异步节点
                    if i == 0:
                        result = await node(*args, **kwargs)
                    else:
                        result = await node(last_result)

                    # 成功执行
                    last_result = result
                    logger.debug(f"Iteration {i + 1} completed successfully")

                except Exception as e:
                    errors.append(e)
                    logger.error(f"Iteration {i + 1} failed: {e}")

                    # 检查是否应该立即停止
                    if stop_on_error:
                        logger.error("Stopping immediately due to stop_on_error=True")
                        # 抛出RepeatStopException，让其被重试机制处理
                        raise LoopControlException(
                            f"Execution stopped due to error at iteration {i + 1}: {e}"
                        ) from e

                    # 继续执行，但使用上一次的成功结果
                    logger.info(
                        f"Continuing with last successful result from iteration {i}"
                    )

            # 正常完成
            logger.info(
                f"Async repeat composition completed. Iterations: {times}, Errors: {len(errors)}"
            )

            return last_result

        return Node(func=async_run, name=composition_name, is_start_node=False)
    else:
        # 同步节点：创建同步执行函数
        def run(*args: Any, **kwargs: Any) -> Any:
            logger.info(
                f"--- Executing Sync Repeat Composition: {composition_name} ---"
            )

            last_result = None
            errors = []

            for i in range(times):
                logger.info(f"  - Iteration {i + 1}/{times}")

                try:
                    # 正常执行同步节点
                    if i == 0:
                        result = node(*args, **kwargs)
                    else:
                        result = node(last_result)

                    # 成功执行
                    last_result = result
                    logger.debug(f"Iteration {i + 1} completed successfully")

                except Exception as e:
                    errors.append(e)
                    logger.error(f"Iteration {i + 1} failed: {e}")

                    # 检查是否应该立即停止
                    if stop_on_error:
                        logger.error("Stopping immediately due to stop_on_error=True")
                        # 抛出RepeatStopException，让其被重试机制处理
                        raise LoopControlException(
                            f"Execution stopped due to error at iteration {i + 1}: {e}"
                        ) from e

                    # 继续执行，但使用上一次的成功结果
                    logger.info(
                        f"Continuing with last successful result from iteration {i}"
                    )

            # 正常完成
            logger.info(
                f"Sync repeat composition completed. Iterations: {times}, Errors: {len(errors)}"
            )

            return last_result

        return Node(func=run, name=composition_name, is_start_node=False)


def node(
    func: Callable | None = None,
    *,
    retry_count: int = 3,
    name: str | None = None,
    retry_delay: float = 1.0,
    exception_types: tuple = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    enable_retry: bool = False,
) -> Node | Callable:
    """
    Decorator: Create Node from function with dependency injection, type validation, and retry mechanism.

    **This is the standard and only recommended way to use dependency injection!**

    Usage Examples:
    ```python
    # Basic sync node
    @node
    def process_data(data: dict, state: dict = Provide[BaseFlowContext.state]) -> dict:
        result = {"processed": data["value"] * 2}
        state['last_result'] = result
        return result

    # Async node with retry
    @node(retry_count=3, retry_delay=0.5)
    async def fetch_data(data_id: str) -> dict:
        # Simulate async data fetching
        await asyncio.sleep(0.1)
        return {"data_id": data_id, "value": f"fetched_{data_id}"}

    # Custom retry configuration
    @node(retry_count=5, retry_delay=2.0, exception_types=(ConnectionError, TimeoutError))
    def external_service_call(data: dict) -> dict:
        return call_external_api(data)

    # Sequential composition (async/sync mixing)
    flow = process_data.then(fetch_api_data).then(external_service_call)
    result = flow({"value": 10})

    # Parallel execution
    parallel_flow = process_data.fan_out_to([
        fetch_api_data,
        external_service_call
    ])
    results = parallel_flow({"value": 10})

    # Fan-out-in pattern
    @node
    def aggregate_results(parallel_results: dict) -> str:
        successful = [r.result for r in parallel_results.values() if r.success]
        return f"Aggregated: {len(successful)} results"

    complete_flow = process_data.fan_out_in([fetch_api_data, external_service_call], aggregate_results)
    final_result = complete_flow({"value": 10})

    # Disable retry for specific node
    @node(enable_retry=False)
    def no_retry_operation(data: dict) -> dict:
        return {"immediate": data}
    ```

    Args:
        func: Function to be decorated
        name: Node identifier name
        retry_count: Maximum retry attempts (default: 3)
        retry_delay: Base retry delay in seconds (default: 1.0)
        exception_types: Tuple of exception types to retry (default: (Exception,))
        backoff_factor: Backoff multiplier for exponential backoff (default: 1.0)
        max_delay: Maximum delay time in seconds (default: 60.0)
        enable_retry: Enable/disable retry mechanism (default: True)

    Returns:
        Node instance or decorator function

    Notes:
        - Supports both sync and async functions with intelligent retry handling
        - Async functions use `asyncio.sleep()` for delays, sync functions use `time.sleep()`
        - Node objects have `is_async` property indicating if they require async execution
        - Smart async detection works with Node objects, decorators, and plain functions
        - If using BaseFlowContext dependency injection, @node decorator is required
        - Container must be wired before usage: `container.wire(modules=[__name__])`
        - Retry mechanism is enabled by default and works for both sync and async nodes
        - Sequential composition automatically handles async/sync mixing via Node.__call__()
        - Parallel execution supports mixed async/sync nodes with auto executor selection
    """
    config = RetryConfig(
        retry_count, retry_delay, exception_types, backoff_factor, max_delay
    )

    @functools.wraps(Node)
    def decorator(f: Callable) -> Node:
        node_name = name or _get_func_name(f, "unnamed_node")

        # 在装饰之前检测原始函数是否为异步函数
        is_original_async = inspect.iscoroutinefunction(f)

        # 使用functools.reduce应用装饰器链
        # inject必须在最外层，确保Provide[...]在验证/重试之前被解析为实际值
        decorators = [
            custom_validate_call(
                validate_return=True,
                config=ConfigDict(arbitrary_types_allowed=True),
                node_name=node_name,
            ),
        ]
        if enable_retry:
            decorators.append(retry_decorator(config=config, node_name=node_name))
        decorators.append(inject)

        decorated_func = functools.reduce(lambda func, deco: deco(func), decorators, f)

        return Node(func=decorated_func, name=node_name, is_async=is_original_async)

    # 支持两种调用方式：@node 和 @node(...)
    if func is None:
        # @node(...) 带参数调用
        return decorator
    else:
        # @node 直接调用
        return decorator(func)


# Export list - 只暴露用户需要的公共接口
__all__ = [
    # 装饰器：创建节点
    "node",
    # 验证装饰器：自定义验证异常处理
    "custom_validate_call",
    # 上下文：自定义依赖注入
    "BaseFlowContext",
    # 并行执行结果模型
    "ParallelResult",
    # 异常类
    "AetherFlowException",
    "ValidationInputException",
    "ValidationOutputException",
    "UserBusinessException",
    "NodeExecutionException",
    "NodeTimeoutException",
    "NodeRetryExhaustedException",
    "LoopControlException",
    # 重试相关
    "RetryConfig",
]
