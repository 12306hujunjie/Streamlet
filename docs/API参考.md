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

将函数转为 `Node` 实例，内置 pydantic 类型校验、按需依赖注入和可选重试。

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

输入校验失败抛出 `ValidationInputException`，返回值校验失败抛出
`ValidationOutputException`。返回值校验基于函数的返回类型注解，支持
`from __future__ import annotations` 下的延迟注解，也会保留
`Annotated[...]` 元数据给 Pydantic 处理。因此可以在返回类型里使用
Pydantic 模型和 `Field` 约束：

```python
from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field
from streamlet import node


class User(BaseModel):
    name: str
    age: int


@node
def create_user() -> User:
    return {"name": "Alice", "age": "30"}


@node
def positive_score() -> Annotated[int, Field(gt=0)]:
    return 100
```

依赖注入在函数签名包含 `Provide[...]` / `Provider[...]` 默认值或
`Annotated[..., Provide[...]]` 元数据时按需启用。

## Node 类

### `then(next_node: Node) -> Node`

顺序连接。前一个节点的输出作为后一个节点的输入。

### `fan_out_to(nodes: list[Node], executor: str = "thread", max_workers: int = None) -> Node`

并行扇出。source 执行后，每个 target 并行执行。返回 `dict[str, ParallelResult]`。
如果后面继续调用 `.then(next_node)`，`next_node` 接收的也是这个原始结果字典，
不是成功结果列表或自动解包后的业务值。fan-out 后继续业务链时，应先调用
`.fan_in(aggregator)` 显式聚合，或使用 `.fan_out_in(...)`。

默认情况下，每个 target 接收相同的 source 输出作为单个位置参数。若 source 返回
`fan_out_args(...)`，则每个参数字典按顺序对应一个 target，并作为该 target 的
`kwargs` 调用。裸 `list[dict]` 会被当成普通业务数据广播，不会触发按 target 展开。

`executor` 取值：
- `"thread"` — 线程池（默认），更适合 I/O 密集或阻塞型任务
- `"async"` — `asyncio.gather` 协程调度，只适合真实 `async` 且会主动
  `await` 的 target
- `"auto"` — 全同步节点选择 `"thread"`；包含异步节点时使用 hybrid 调度，
  async target 留在 event loop，同步 target 放入线程池执行

注意：`"async"` executor 不会把同步 target 自动放入线程池。同步 target 会在
当前 event loop 内直接执行；CPU 计算或阻塞 I/O 不会获得协程并发，还会阻塞
event loop。同步或阻塞型 target 请使用默认 `"thread"`，或在不确定时使用
`"auto"`。

### `fan_out_args(*items: dict[str, Any]) -> FanOutArgs`

显式 fan-out 参数协议。每个 `dict` 对应 `fan_out_to` 中同位置 target 的
`kwargs`。`items` 数量必须等于 target 数量，否则在运行时抛出 `ValueError`。

```python
from streamlet import fan_out_args, node

@node
def source(user_id: str):
    return fan_out_args(
        {"user_id": user_id, "limit": 10},
        {"user_id": user_id, "include_archived": False},
    )

flow = source.fan_out_to([fetch_orders, fetch_profile])
```

### `fan_in(aggregator: Node) -> Node`

聚合并行结果。aggregator 接收 `dict[str, ParallelResult]` 参数。

`fan_in` 是 fan-out 后回到普通业务链的显式入口：aggregator 负责检查
`ParallelResult.success`、处理失败分支，并返回下游 `.then(...)` 真正需要的业务值。

### `fan_out_in(targets: list[Node], aggregator: Node, executor: str = "thread", max_workers: int = None) -> Node`

`fan_out_to` + `fan_in` 组合，一步完成。

### `branch_on(conditions: dict[Any, Node]) -> Node`

条件分支。条件节点返回值作为路由键，匹配对应分支节点执行。

`branch_on` 只向条件节点传递调用输入；选中的分支节点以零参数执行。
框架不会把原始输入或条件返回值传给分支。分支节点需要业务数据时，
可通过依赖注入读取 `BaseFlowContext.context`。

### `repeat(times: int, stop_on_error: bool = False, *, input_mode: RepeatInputMode = RepeatInputMode.PREVIOUS_RESULT) -> Node`

重复执行节点，并返回最后一次成功执行的结果。

`input_mode` 必须是 `RepeatInputMode` 枚举：

| 模式 | 行为 |
|------|------|
| `RepeatInputMode.PREVIOUS_RESULT` | 默认。第 1 轮执行 `node(*args, **kwargs)`；之后把每轮成功返回值作为下一轮的单个位置参数。返回 `call_args(...)` 时，使用其中显式声明的 `*args/**kwargs` |
| `RepeatInputMode.SAME_INPUT` | 每一轮都执行 `node(*args, **kwargs)`，重复使用最初传入的参数 |

```python
from streamlet import CallArgs, RepeatInputMode, call_args, node

@node
def inc(value: int) -> int:
    return value + 1

assert inc.repeat(3)(0) == 3

@node
def step(value: int, factor: int = 1) -> CallArgs:
    return call_args(value * factor, factor=factor)

assert step.repeat(3)(2, factor=10) == call_args(2000, factor=10)

@node
def load(source: str, limit: int = 10) -> list[str]:
    return fetch_batch(source, limit=limit)

flow = load.repeat(3, input_mode=RepeatInputMode.SAME_INPUT)
result = flow("orders", limit=50)
```

`call_args(*args, **kwargs)` 会创建显式的下一轮调用参数。`PREVIOUS_RESULT` 不会自动展开普通 `tuple` 或 `dict`；这些类型会被视为业务返回值，作为一个位置参数传入下一轮。

`stop_on_error=False`（默认）时，单次迭代失败会记录 warning 并继续循环；下一次迭代仍按 `input_mode` 决定输入：`PREVIOUS_RESULT` 使用最近一次成功返回值推导出的下一轮调用参数，`SAME_INPUT` 继续使用原始参数。若所有迭代都失败，返回 `None`。

`stop_on_error=True` 时，任一迭代失败都会立即抛出 `LoopControlException`。

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
| `context` | `ContextVarProvider[dict]` | 执行上下文；线程/协程隔离，并在 fan-out 分支中复制父上下文 |

节点通过 `Provide[BaseFlowContext.context]` 注入依赖：

```python
from streamlet import BaseFlowContext, node
from dependency_injector.wiring import Provide

container = BaseFlowContext()

@node
def my_node(context: dict = Provide[BaseFlowContext.context]) -> dict:
    return {"data": context["key"]}

container.wire(modules=[__name__])  # 必须在 @node 定义之后调用
```

fan-out 分支复制父上下文时，默认只浅拷贝顶层 `dict`。这会隔离顶层 key 的新增、
删除和替换，但不会隔离嵌套的 `list` / `dict` / `set` 等可变 value；这些对象
仍会在分支之间共享引用。

## ContextVarProvider

```python
ContextVarProvider(
    default_factory: Callable[[], Any] = dict,
    copy_policy: str = "shallow",
)
```

`ContextVar` 驱动的 provider，支持线程和协程隔离。`BaseFlowContext.context`
默认使用 `ContextVarProvider(dict)`。

`copy_policy` 取值：
- `"shallow"` — 默认策略；fan-out 隔离时只浅拷贝顶层 `dict`
- `"strict"` — fan-out 隔离时拒绝嵌套可变值和非 `dict` 可变 context 值，不做
  递归深拷贝

`strict` 的目标是提前暴露浅拷贝无法隔离的共享状态风险，而不是自动复制这些
对象。直接作为 value 的 `list` / `dict` / `set` 会被拒绝，包含在 `tuple` /
`frozenset` 等不可变容器里的可变值也会被拒绝，例如
`{"items": ("header", [])}`。

需要让嵌套可变状态在 fan-out 前失败时，可定义自定义 context 容器：

```python
from streamlet import BaseFlowContext, ContextVarProvider

class StrictFlowContext(BaseFlowContext):
    context = ContextVarProvider(dict, copy_policy="strict")
```

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
| `NodeExecutionException` | `StreamletException` | `False` | 节点执行失败 |
| `NodeTimeoutException` | `NodeExecutionException` | `False` | 执行超时 |
| `NodeRetryExhaustedException` | `NodeExecutionException` | `False` | 重试耗尽 |
| `LoopControlException` | `StreamletException` | `False` | `repeat(stop_on_error=True)` 触发 |

`StreamletException` 构造：`__init__(message: str, node_name: str | None = None, **kwargs)`。`kwargs` 存入 `context` 字典。
