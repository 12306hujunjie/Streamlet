# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 环境 & 命令

必须使用 PDM 管理虚拟环境和依赖：

```bash
pdm install              # 安装依赖
pdm run pytest           # 运行所有测试
pdm run pytest tests/test_fan_out.py -v   # 单个测试文件
pdm run pytest --cov=src/streamlet        # 带覆盖率
pdm run ruff check src/ tests/            # lint
pdm run ruff format src/ tests/           # 格式化
pdm run mypy src/streamlet/              # 类型检查
```

## 项目架构

**Streamlet** 是一个声明式数据流处理框架。用户通过 fluent 方法链 (`.then()` / `.fan_out_to()` / `.branch_on()` / `.repeat()`) 表达业务逻辑，框架自动处理 async/sync 混合执行、并行调度、重试和依赖注入。

### 分层架构

```
用户 API 层:     __init__.py  (纯重导出，22 个公开符号)
入口 & 组合层:   node.py     (Node 类 + @node 装饰器)
内部组合层:      graph.py    (Pipeline / Parallel / Conditional / Repeat / FanIn — 不对外暴露)
执行引擎层:      executor.py (SyncExecutor / AsyncExecutor + ParallelResult / FanOutArgs)
基础设施:        context.py  (BaseFlowContext DI 容器 + pydantic 校验包裹)
                 retry.py    (指数退避重试)
                 exceptions.py (8 个异常类继承层次)
```

### 模块依赖方向（单向，无循环）

```
node.py → graph.py → executor.py
   ↓         ↓           ↓
context.py   exceptions.py   retry.py
```

### 核心设计模式

- **组合优于继承**: `Node._func` 持有 Graph 内部类实例，用户永远看不见 Pipeline/Parallel 等
- **双重执行**: `SyncExecutor`（ThreadPoolExecutor 扇出）+ `AsyncExecutor`（asyncio.gather 扇出），`Parallel` 通过 `"thread"` / `"async"` / `"auto"` 选择策略
- **ContextVar 状态隔离**: fan-out 时 `capture_context()` 快照 → 每个 worker 线程/协程 `apply_context()` 恢复浅拷贝，分支间互不污染
- **异常驱动的重试门控**: 异常类通过 `retryable` 类属性声明可重试性，`RetryConfig.should_retry()` 据此决定是否重试
- **FanOutArgs 哨兵协议**: source 返回 `fan_out_args(dict1, dict2)` 时为每个 target 传递独立 kwargs；返回普通值则广播同一参数给所有 target

### 关键代码路径

| 操作 | 执行路径 |
|------|---------|
| `node_func(x)` | `@node` 装饰器包裹层 → `Node.__call__` → `Node._execute` |
| `.then(right)` | 创建 `Pipeline(left=当前, right)` 包装为 `Node` |
| `.fan_out_to(targets)` | 创建 `Parallel(source=当前, targets)` → `SyncExecutor.gather` 或 `AsyncExecutor.agather` |
| fan-out 上下文隔离 | `capture_context()` → 每个 worker `apply_context(snapshot)` → 执行 → 返回 `ParallelResult` |
| `branch_on({k: node})` | `Conditional` 执行 condition → 用返回值查 `branches` dict → 执行选中分支 |

## 技术约束

- **Python 3.10+**，必须用 PDM 管理依赖
- **所有函数必须有类型注解**（mypy strict 模式）
- **测试节点优先定义在模块级别**，使用 `@node` 装饰器而非直接实例化 `Node`
- **Node 不支持 pickle 序列化**，不要依赖 `ProcessPoolExecutor` 或进程池传递 `Node` 实例
- Graph 内部类（`Pipeline` / `Parallel` / `Conditional` / `Repeat` / `FanIn`）**绝不对用户暴露**，仅由 `Node` fluent 方法内部创建
- 新依赖需添加到 `pyproject.toml` 并评估必要性
- 异常类继承自 `StreamletException`，用户业务异常继承自 `UserBusinessException`（默认 `retryable=True`）
