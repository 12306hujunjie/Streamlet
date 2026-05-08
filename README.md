# Streamlet - 智能流式数据处理框架

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
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
| `.repeat(times)` | 重复执行 | `a.repeat(3)(data)` |

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
from streamlet import node

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

### 条件流：分支路由 + 依赖注入

```python
from streamlet import BaseFlowContext, node
from dependency_injector.wiring import Provide

container = BaseFlowContext()

@node
def evaluate(data: dict) -> str:
    return "pass" if data["score"] >= 60 else "fail"

@node
def handle_pass(state: dict = Provide[BaseFlowContext.state]) -> dict:
    return {"result": "pass", "score": state["score"]}

@node
def handle_fail(state: dict = Provide[BaseFlowContext.state]) -> dict:
    return {"result": "fail", "score": state["score"]}

container.wire(modules=[__name__])
container.state()["score"] = 75

flow = evaluate.branch_on({"pass": handle_pass, "fail": handle_fail})
print(flow({"score": 75}))  # {"result": "pass", "score": 75}
```

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
- **dependency-injector** — 依赖注入与线程安全状态管理
- **pydantic v2** — 类型校验

核心模块：`asyncio` | `threading` | `concurrent.futures`

## 文档

- [API 参考](docs/API参考.md)
- [CLAUDE.md](CLAUDE.md) — 开发指南
- `tests/` — 测试用例与使用示例

## 许可证

MIT — 详见 [LICENSE](LICENSE)
