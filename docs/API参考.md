# Streamlet API 参考

## @node 装饰器

```signature
node(
    func: Callable[..., Any] | None = None,
    *,
    retry_count: int = 3,
    name: str | None = None,
    timeout: float | None = None,
    retry_delay: float = 1.0,
    exception_types: tuple[type[Exception], ...] = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    enable_retry: bool = False,
) -> Node | Callable[[Callable[..., Any]], Node]
```

将函数转为 `Node` 实例，内置 pydantic 类型校验、按需依赖注入和可选重试。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `func` | `Callable[..., Any] \| None` | `None` | 待包装函数；省略时返回装饰器 |
| `name` | `str \| None` | 函数名 | 节点标识 |
| `timeout` | `float \| None` | `None` | 单次节点调用超时时间（秒） |
| `retry_count` | `int` | `3` | 最大重试次数 |
| `retry_delay` | `float` | `1.0` | 初始重试延迟（秒） |
| `exception_types` | `tuple` | `(Exception,)` | 可重试异常类型 |
| `backoff_factor` | `float` | `1.0` | 指数退避乘数，`delay * factor^attempt` |
| `max_delay` | `float` | `60.0` | 重试延迟上限（秒） |
| `enable_retry` | `bool` | `False` | 是否启用重试 |

作为装饰器支持 `@node`（无参数）和 `@node(name="n")`（带参数）；也可直接
调用 `node(func)` 或 `node(func, name="n", ...)`，并立即返回 `Node`。
`timeout` 必须为正数；节点调用超过该时间会抛出 `NodeTimeoutException`，
异常包含 `node_name` 和 `timeout_seconds`。同步函数通过 `func-timeout` 执行，
异步函数通过 `asyncio.wait_for` 执行。启用重试时，`timeout` 是整次节点调用的
总预算，包含全部重试尝试和重试间隔。同步超时是 Python 级中断，不是进程级
强杀；如果用户函数长时间停在不可中断的 C 扩展、系统调用，或主动吞掉超时异常，
底层执行可能无法立即停止。

同步 `timeout` 会在 `func-timeout` 的工作线程中执行节点调用。Streamlet 会把
自身 `ContextVarProvider` 的当前快照传播到该工作线程；若调用本身已经位于
fan-out 线程池或其他用户线程中，传播的是该调用线程当时的快照。`dict` 类型的
context 值只浅拷贝顶层字典，非 `dict` 对象按原引用传播。对象是否可跨线程使用
由用户保证；线程绑定资源（例如部分 DB session、request scoped 对象、依赖当前
event loop 的 client）不应直接放入同步 timeout 节点的 context。
同步 timeout 的上下文传播同样会执行 `copy_policy="strict"` 校验，避免工作线程
共享嵌套可变状态。

重试配置只在 `enable_retry=True` 时构造、校验并执行。未启用重试时，
`retry_count`、`retry_delay`、`exception_types`、`backoff_factor` 和
`max_delay` 会被忽略；`timeout` 仍始终校验。

输入校验失败抛出 `ValidationInputException`，返回值校验失败抛出
`ValidationOutputException`。返回值校验基于函数的返回类型注解，支持
`from __future__ import annotations` 下的延迟注解，也会保留
`Annotated[...]` 元数据给 Pydantic 处理。因此可以在返回类型里使用
Pydantic 模型和 `Field` 约束：

下面用返回 `Any` 的 helper 模拟外部未校验数据，类型转换由节点的输出校验完成。

```python
from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field
from streamlet import node


class User(BaseModel):
    name: str
    age: int


def load_user_payload() -> Any:
    return {"name": "Alice", "age": "30"}


@node
def create_user() -> User:
    return load_user_payload()


def load_score() -> Any:
    return "100"


@node
def positive_score() -> Annotated[int, Field(gt=0)]:
    return load_score()


assert create_user() == User(name="Alice", age=30)
assert positive_score() == 100
```

依赖注入在函数签名包含 `Provide[...]` / `Provider[...]` 默认值或
`Annotated[..., Provide[...]]` 元数据时按需启用。

## Node 类

### `then(other: Node) -> Node`

```signature
then(
    other: Node,
) -> Node
```

顺序连接。前一个节点的输出作为后一个节点的输入。

### `fan_out_to(nodes: list[Node], executor: str = "thread", max_workers: int | None = None) -> Node`

```signature
fan_out_to(
    nodes: list[Node],
    executor: str = "thread",
    max_workers: int | None = None,
) -> Node
```

并行扇出。source 执行后，每个 target 并行执行。返回 `dict[str, ParallelResult]`。
如果后面继续调用 `.then(next_node)`，`next_node` 接收的也是这个原始结果字典，
不是成功结果列表或自动解包后的业务值。fan-out 后继续业务链时，应先调用
`.fan_in(aggregator)` 显式聚合，或使用 `.fan_out_in(...)`。

默认情况下，每个 target 接收相同的 source 输出作为单个位置参数。若 source 返回
`fan_out_args(...)`，则每个参数字典按顺序对应一个 target，并作为该 target 的
`kwargs` 调用。裸 `list[dict]` 会被当成普通业务数据广播，不会触发按 target 展开。

target 抛出的普通 `Exception` 会被包装为 `ParallelResult(success=False)`，
并保留 `error` 与 `error_traceback`；这种普通 target 失败不阻断其他 target。
取消或进程级中断不会被包装：`asyncio.CancelledError`、`KeyboardInterrupt`、
`SystemExit` 会向调用方传播，因此本次 fan-out 不返回结果字典。传播不保证取消
已经开始执行的 sibling target；这些 target 仍可能继续运行或完成。

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

@node
def fetch_orders(user_id: str, limit: int) -> list[str]:
    return [f"order:{user_id}:{index}" for index in range(limit)]

@node
def fetch_profile(user_id: str, include_archived: bool) -> dict:
    return {"user_id": user_id, "include_archived": include_archived}

flow = source.fan_out_to([fetch_orders, fetch_profile])
results = flow("u-1")
assert results["fetch_orders"].result == [
    f"order:u-1:{index}" for index in range(10)
]
assert results["fetch_profile"].result == {
    "user_id": "u-1",
    "include_archived": False,
}
```

### `fan_in(aggregator: Node) -> Node`

```signature
fan_in(
    aggregator: Node,
) -> Node
```

聚合并行结果。aggregator 接收 `dict[str, ParallelResult]` 参数。

`fan_in` 是 fan-out 后回到普通业务链的显式入口：aggregator 负责检查
`ParallelResult.success`、处理失败分支，并返回下游 `.then(...)` 真正需要的业务值。

### `fan_out_in(targets: list[Node], aggregator: Node, executor: str = "thread", max_workers: int | None = None) -> Node`

```signature
fan_out_in(
    targets: list[Node],
    aggregator: Node,
    executor: str = "thread",
    max_workers: int | None = None,
) -> Node
```

`fan_out_to` + `fan_in` 组合，一步完成。

### `branch_on(conditions: dict[Any, Node]) -> Node`

```signature
branch_on(
    conditions: dict[Any, Node],
) -> Node
```

条件分支。条件节点返回值作为路由键，匹配对应分支节点执行。

`branch_on` 只向条件节点传递调用输入；选中的分支节点以零参数执行。
框架不会把原始输入或条件返回值传给分支。分支节点需要业务数据时，
可通过依赖注入读取 `BaseFlowContext.context`。

### `repeat(times: int, stop_on_error: bool = False, *, input_mode: RepeatInputMode = RepeatInputMode.PREVIOUS_RESULT) -> Node`

```signature
repeat(
    times: int,
    stop_on_error: bool = False,
    *,
    input_mode: RepeatInputMode = RepeatInputMode.PREVIOUS_RESULT,
) -> Node
```

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

load_calls: list[tuple[str, int]] = []

@node
def load(source: str, limit: int = 10) -> list[str]:
    load_calls.append((source, limit))
    return [f"{source}:{index}" for index in range(limit)]

flow = load.repeat(
    3,
    stop_on_error=True,
    input_mode=RepeatInputMode.SAME_INPUT,
)
result = flow("orders", limit=2)
assert result == ["orders:0", "orders:1"]
assert load_calls == [("orders", 2)] * 3
```

`call_args(*args, **kwargs)` 会创建显式的下一轮调用参数。`PREVIOUS_RESULT` 不会自动展开普通 `tuple` 或 `dict`；这些类型会被视为业务返回值，作为一个位置参数传入下一轮。

`stop_on_error=False`（默认）时，单次迭代失败会记录 warning 并继续循环；下一次迭代仍按 `input_mode` 决定输入：`PREVIOUS_RESULT` 使用最近一次成功返回值推导出的下一轮调用参数，`SAME_INPUT` 继续使用原始参数。若所有迭代都失败，返回 `None`。

`stop_on_error=True` 时，任一迭代失败都会立即抛出 `LoopControlException`。

## RetryConfig

```signature
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

```signature
class ParallelResult:
    node_name: str
    success: bool
    result: Any = None
    error: str | None = None
    error_traceback: str | None = None
    execution_time: float | None = None
```

`fan_out_to` 返回 `dict[str, ParallelResult]`，键为节点名（重复时自动加后缀 `[n]`）。
`ParallelResult(success=False)` 只表示 target 的普通业务异常被包装；取消、
`KeyboardInterrupt`、`SystemExit` 等 `BaseException` 路径会直接传播，不会生成
`ParallelResult`，fan-out 也不会返回结果字典。已经开始执行的 sibling target
不保证被取消，仍可能继续运行或完成。

## BaseFlowContext

依赖注入容器，继承 `dependency-injector` 的 `DeclarativeContainer`。

```python compile-only
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
container.context()["key"] = "value"
assert my_node() == {"data": "value"}
```

fan-out 分支复制父上下文时，默认只浅拷贝顶层 `dict`。这会隔离顶层 key 的新增、
删除和替换，但不会隔离嵌套的 `list` / `dict` / `set` 等可变 value；这些对象
仍会在分支之间共享引用。

## ContextVarProvider

```signature
ContextVarProvider(
    default_factory: Callable[[], Any] = dict,
    copy_policy: str = "shallow",
)
```

`ContextVar` 驱动的 provider，支持线程和协程隔离。`BaseFlowContext.context`
默认使用 `ContextVarProvider(dict)`。

`copy_policy` 取值：
- `"shallow"` — 默认策略；跨执行上下文传播时只浅拷贝顶层 `dict`
- `"strict"` — fan-out 或同步 timeout 传播时拒绝嵌套可变值和非 `dict` 可变
  context 值，不做递归深拷贝

`strict` 是跨执行上下文传播的风险门禁，目标是提前暴露浅拷贝无法隔离的共享状态
风险，而不是自动复制这些对象。直接作为 value 的 `list` / `dict` / `set` 会被
拒绝，包含在 `tuple` / `frozenset` 等不可变容器里的可变值也会被拒绝，例如
`{"items": ("header", [])}`。普通节点调用不会触发这项校验。

需要让嵌套可变状态在 fan-out 前失败时，可定义自定义 context 容器：

```python
from streamlet import BaseFlowContext, ContextVarProvider

class StrictFlowContext(BaseFlowContext):
    context = ContextVarProvider(dict, copy_policy="strict")
```

## `streamlet.retry.retry_decorator`

```signature
retry_decorator(
    config: RetryConfig,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]
```

模块级重试装饰器，未从 `streamlet` 顶层重导出；`@node` 在
`enable_retry=True` 时会调用它。支持同步和异步函数。

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
