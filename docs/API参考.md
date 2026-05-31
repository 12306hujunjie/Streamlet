# Streamlet API 参考

## @node 装饰器

```python
@node(
    name: str | None = None,
    retry_count: int = 3,
    retry_delay: float = 1.0,
    exception_types: tuple = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    enable_retry: bool = False,
)
```

将函数转为 `Node` 实例，内置依赖注入、pydantic 类型校验和可选重试。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | `str \| None` | 函数名 | 节点标识 |
| `retry_count` | `int` | `3` | 最大重试次数，需 `enable_retry=True` |
| `retry_delay` | `float` | `1.0` | 初始重试延迟（秒） |
| `exception_types` | `tuple` | `(Exception,)` | 可重试异常类型 |
| `backoff_factor` | `float` | `1.0` | 指数退避乘数，`delay * factor^attempt` |
| `max_delay` | `float` | `60.0` | 重试延迟上限（秒） |
| `enable_retry` | `bool` | `False` | 是否启用重试 |

支持两种调用方式：`@node`（无参数）和 `@node(name="n")`（带参数）。

## Node 类

### `then(next_node: Node) -> Node`

顺序连接。前一个节点的输出作为后一个节点的输入。

### `fan_out_to(targets: list[Node], executor: str = "thread", max_workers: int = None) -> Node`

并行扇出。source 执行后，每个 target 接收相同的 source 输出并行执行。返回 `dict[str, ParallelResult]`。

`executor` 取值：
- `"thread"` — 线程池（默认），更适合 I/O 密集或阻塞型任务
- `"async"` — 协程并发，适合 I/O 密集任务
- `"auto"` — 根据节点类型自动选择

### `fan_in(aggregator: Node) -> Node`

聚合并行结果。aggregator 接收 `dict[str, ParallelResult]` 参数。

### `fan_out_in(targets: list[Node], aggregator: Node, executor: str = "thread", max_workers: int = None) -> Node`

`fan_out_to` + `fan_in` 组合，一步完成。

### `branch_on(conditions: dict[Any, Node]) -> Node`

条件分支。条件节点返回值作为路由键，匹配对应分支节点执行。

分支节点通过依赖注入（推荐 `Provide[BaseFlowContext.current_state]`）获取数据，因为 `branch_on` 不向分支节点传递参数。

### `repeat(times: int, stop_on_error: bool = False) -> Node`

重复执行。`stop_on_error=False`（默认）时，错误后继续使用上一次成功结果；`stop_on_error=True` 时立即抛出 `LoopControlException`。

## RetryConfig

```python
RetryConfig(
    retry_count: int = 3,
    retry_delay: float = 1.0,
    exception_types: tuple[type[Exception], ...] = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
)
```

### `should_retry(exception: Exception) -> bool`

判断异常是否应重试：优先检查 `exception.retryable` 属性，否则按 `exception_types` 判断。

### `get_delay(attempt: int) -> float`

计算第 N 次重试的等待时间：`retry_delay * backoff_factor^attempt`，上限 `max_delay`。

## ParallelResult

```python
@dataclass
class ParallelResult:
    node_name: str
    success: bool
    result: Any = None
    error: str | None = None
    error_traceback: str | None = None
    execution_time: float | None = None
```

`fan_out_to` 返回 `dict[str, ParallelResult]`，键为节点名（重复时自动加后缀 `[n]`）。

## BaseFlowContext

依赖注入容器，继承 `dependency-injector` 的 `DeclarativeContainer`。

```python
container = BaseFlowContext()
container.wire(modules=[__name__])
```

| 提供者 | 类型 | 说明 |
|--------|------|------|
| `state` | `ThreadLocalSingleton[dict]` | 线程隔离状态 |
| `context` | `ThreadLocalSingleton[dict]` | 线程隔离上下文 |
| `shared_data` | `Singleton[dict]` | 全线程共享 |
| `async_state` | `ContextVarProvider[dict]` | 协程安全状态 |
| `async_context` | `ContextVarProvider[dict]` | 协程安全上下文 |
| `current_state` | `Provider[dict]` | 推荐入口：sync→thread local；async→contextvar |
| `current_context` | `Provider[dict]` | 推荐入口：sync→thread local；async→contextvar |

节点通过 `Provide[BaseFlowContext.current_state]`（推荐）注入依赖：

```python
from streamlet import BaseFlowContext, node
from dependency_injector.wiring import Provide

container = BaseFlowContext()

@node
def my_node(state: dict = Provide[BaseFlowContext.current_state]) -> dict:
    return {"data": state["key"]}

container.wire(modules=[__name__])  # 必须在 @node 定义之后调用
```

## custom_validate_call

```python
custom_validate_call(
    validate_return: bool = True,
    config: ConfigDict | None = None,
    node_name: str | None = None,
) -> Callable
```

验证装饰器。输入校验失败抛出 `ValidationInputException`，返回值校验失败抛出 `ValidationOutputException`。自动识别 `Provide` 参数并跳过其默认值填充。

## retry_decorator

```python
retry_decorator(
    config: RetryConfig,
    node_name: str | None = None,
) -> Callable
```

独立重试装饰器（`@node` 内部使用 `enable_retry=True` 时自动调用）。支持同步和异步函数。

## 异常类

| 异常 | 基类 | `retryable` | 说明 |
|------|------|------------|------|
| `StreamletException` | `Exception` | `False` | 框架基类异常 |
| `ValidationInputException` | `StreamletException` | `False` | 输入参数校验失败 |
| `ValidationOutputException` | `StreamletException` | `False` | 返回值校验失败 |
| `UserBusinessException` | `StreamletException` | `True` | 用户业务异常，可覆盖 |
| `NodeExecutionException` | `StreamletException` | - | 节点执行失败 |
| `NodeTimeoutException` | `NodeExecutionException` | - | 执行超时 |
| `NodeRetryExhaustedException` | `NodeExecutionException` | - | 重试耗尽 |
| `LoopControlException` | `StreamletException` | - | `repeat(stop_on_error=True)` 触发 |

`StreamletException` 构造：`__init__(message: str, node_name: str | None = None, **kwargs)`。`kwargs` 存入 `context` 字典。
