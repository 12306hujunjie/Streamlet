# Streamlet - 智能流式数据处理框架

[![Tests](https://github.com/12306hujunjie/Streamlet/actions/workflows/test.yml/badge.svg)](https://github.com/12306hujunjie/Streamlet/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/12306hujunjie/Streamlet/branch/main/graph/badge.svg)](https://codecov.io/gh/12306hujunjie/Streamlet)
[![PyPI](https://img.shields.io/pypi/v/streamlet-py.svg)](https://pypi.org/project/streamlet-py/)
[![Python](https://img.shields.io/pypi/pyversions/streamlet-py.svg)](https://pypi.org/project/streamlet-py/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**声明式数据流处理框架：用方法链表达业务逻辑，框架自动处理异步/同步混合执行、并行调度和重试。**

- 🎯 **声明式工作流**：`.then()` `.fan_out_to()` `.fan_in()` `.branch_on()` `.repeat()` 方法链构建数据流
- 🤖 **智能异步执行**：自动检测 async/sync 函数并选择正确的执行策略，无需手动协调
- 🔗 **@node 装饰器**：任意函数变为可组合节点，内置 pydantic 类型校验和依赖注入
- 🛡️ **重试机制**：基于异常分类的可配置指数退避重试

## 快速开始

```bash
pip install streamlet-py
```

```python
from streamlet import node

@node
def double(x: int) -> int:
    return x * 2

@node
def add_ten(x: int) -> int:
    return x + 10

result = double.then(add_ten)(5)  # 20
```

## 核心 API

| 方法 | 功能 | 示例 |
|------|------|------|
| `.then(node)` | 顺序连接 | `a.then(b)(data)` |
| `.fan_out_to([nodes], executor="thread")` | 并行分发 | `a.fan_out_to([b, c])()` |
| `.fan_in(aggregator)` | 聚合并行结果 | `flow.fan_in(merge)()` |
| `.fan_out_in([nodes], agg)` | 扇出 + 聚合 | `a.fan_out_in([b, c], merge)()` |
| `.branch_on({key: node})` | 条件分支 | `a.branch_on({True: b, False: c})()` |
| `.repeat(times, input_mode=RepeatInputMode.PREVIOUS_RESULT)` | 重复执行 | `a.repeat(3)(data)` |

`fan_out_to` / `fan_out_in` 的 `executor` 用于选择并行调度策略：
`"thread"` 使用线程池，适合同步或阻塞型 target；`"async"` 使用
`asyncio.gather`，只适合真实 `async` 且会主动 `await` 的 target。同步
target 在 `"async"` executor 下会直接运行在当前 event loop 中，不会自动放入
线程池，CPU 或阻塞 I/O 会阻塞 event loop。`"auto"` 会在全同步节点时选择
`"thread"`；在包含异步节点时使用 hybrid 调度：async target 留在 event loop，
同步 target 放入线程池执行。

`fan_out_to(...).then(...)` 不会把成功结果列表传给下游；`.then()` 接收到的是
原始的 `dict[str, ParallelResult]`。fan-out 后如果要继续处理业务结果，应先用
`.fan_in(aggregator)` 显式聚合，或直接使用 `.fan_out_in(...)`。

fan-out 的失败模型只包装普通业务异常：target 抛出的 `Exception` 会变成
`ParallelResult(success=False, error=...)`；这种普通 target 失败不阻断其他
target。
`asyncio.CancelledError`、`KeyboardInterrupt`、`SystemExit` 等取消或进程级
中断不会被包装，会向外传播并中断整个 fan-out。

`repeat` 默认使用 `RepeatInputMode.PREVIOUS_RESULT`：第 1 轮执行
`node(*args, **kwargs)`，之后把每轮成功返回值作为下一轮的单个位置参数。
如果需要为下一轮指定多个位置参数或关键字参数，返回 `call_args(...)`。
如果每轮都要使用最初传入的参数，使用 `RepeatInputMode.SAME_INPUT`：

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
def collect(source: str, limit: int = 10) -> list[str]:
    return fetch_batch(source, limit=limit)

flow = collect.repeat(3, input_mode=RepeatInputMode.SAME_INPUT)
results = flow("orders", limit=50)
```

`@node` 内置 Pydantic 输入和返回值校验。返回值校验支持
`from __future__ import annotations` 下的延迟注解，并保留
`Annotated[...]` 元数据，例如 `Field(gt=0)`；校验失败分别抛出
`ValidationInputException` 或 `ValidationOutputException`。更多细节见
[API 参考](docs/API参考.md#node-装饰器) 和
[Pydantic TypeAdapter](https://docs.pydantic.dev/latest/concepts/type_adapter/)。

## 示例

### 顺序流：ETL 管道

```python
from streamlet import node
import asyncio

@node
async def fetch_data(source: str) -> dict:
    await asyncio.sleep(0.1)
    return {"value": 100, "source": source}

@node
def validate(data: dict) -> dict:
    if data["value"] <= 0:
        raise ValueError("invalid value")
    return data

@node
def enrich(data: dict) -> dict:
    return {**data, "doubled": data["value"] * 2}

pipeline = fetch_data.then(validate).then(enrich)

async def main():
    result = await pipeline("db")
    print(result)  # {"value": 100, "source": "db", "doubled": 200}

asyncio.run(main())
```

### 并行流：扇出 + 聚合

```python
from streamlet import fan_out_args, node

@node
def source(x: int) -> dict:
    return {"value": x}

@node
def multiply(data: dict) -> int:
    return data["value"] * 2

@node
def add_ten(data: dict) -> int:
    return data["value"] + 10

@node
def aggregate(results: dict) -> dict:
    values = [r.result for r in results.values() if r.success]
    return {"total": sum(values), "results": values}

workflow = source.fan_out_to([multiply, add_ten], executor="thread").fan_in(aggregate)
result = workflow(5)
print(result)  # {"total": 25, "results": [10, 15]}
```

注意：`fan_out_to([multiply, add_ten]).then(next_node)` 会把
`dict[str, ParallelResult]` 原样传给 `next_node`，不会自动提取成功结果。
继续业务链时通常应写成 `fan_out_to(...).fan_in(aggregate).then(next_node)`。

source 返回普通值时，所有 target 接收同一个单参数；需要为不同 target
传递不同 kwargs 时，返回显式的 `fan_out_args`：

```python
@node
def route(user_id: str):
    return fan_out_args(
        {"user_id": user_id, "limit": 10},
        {"user_id": user_id, "include_archived": False},
    )

flow = route.fan_out_to([fetch_orders, fetch_profile])
```

### 条件流：分支路由 + 依赖注入

```python
from streamlet import BaseFlowContext, node
from dependency_injector.wiring import Provide

container = BaseFlowContext()

@node
def evaluate(data: dict) -> str:
    return "pass" if data["score"] >= 60 else "fail"

@node
def handle_pass(state: dict = Provide[BaseFlowContext.context]) -> dict:
    return {"result": "pass", "score": state["score"]}

@node
def handle_fail(state: dict = Provide[BaseFlowContext.context]) -> dict:
    return {"result": "fail", "score": state["score"]}

container.wire(modules=[__name__])
container.context()["score"] = 75

flow = evaluate.branch_on({"pass": handle_pass, "fail": handle_fail})
print(flow({"score": 75}))  # {"result": "pass", "score": 75}
```

`branch_on` 的语义是：条件节点接收调用输入并返回路由键，选中的分支节点
以零参数执行。框架不会把原始输入或条件返回值传给分支；分支需要业务数据时，
应通过 `BaseFlowContext.context` 等依赖注入方式读取。

fan-out 分支会复制父级 `BaseFlowContext.context` 后再执行，默认策略只浅拷贝
顶层 `dict`：新增或替换顶层 key 时分支互不影响；如果 value 本身是嵌套
`list` / `dict` / `set` 等可变对象，仍会共享同一个对象引用。需要提前拒绝这类
嵌套可变状态时，可以定义显式的 strict context：

```python
from streamlet import BaseFlowContext, ContextVarProvider

class StrictFlowContext(BaseFlowContext):
    context = ContextVarProvider(dict, copy_policy="strict")
```

`copy_policy="strict"` 不会递归深拷贝 context；它会在 fan-out 隔离时检测并拒绝
会被浅拷贝共享的可变值，让共享状态风险尽早暴露。直接的 `list` / `dict` /
`set` 会被拒绝，藏在 `tuple` / `frozenset` 等不可变容器里的可变值也会被拒绝，
例如 `{"items": ("header", [])}`。

### 重试机制

```python
from streamlet import node

@node(retry_count=3, retry_delay=0.5, backoff_factor=2.0, enable_retry=True)
def external_call(x: int) -> int:
    # 失败时自动重试，延迟按 0.5s → 1.0s → 2.0s 指数增长
    return call_external_api(x)
```

## 开发环境

```bash
git clone https://github.com/12306hujunjie/Streamlet.git
cd Streamlet

pdm install

pdm run pytest                                   # 运行测试
pdm run pytest --cov=src/streamlet              # 覆盖率
pdm run ruff check src/ tests/                   # 代码检查
pdm run mypy src/streamlet/                     # 类型检查
```

## 技术栈

- **Python 3.10+**
- **dependency-injector** — 依赖注入与 ContextVar 隔离状态管理
- **pydantic v2** — 类型校验

核心模块：`asyncio` | `threading` | `concurrent.futures`

## 文档

- [API 参考](docs/API参考.md)
- [CLAUDE.md](CLAUDE.md) — 开发指南
- `tests/` — 测试用例与使用示例

## 许可证

MIT — 详见 [LICENSE](LICENSE)
