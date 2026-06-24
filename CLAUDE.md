# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Think carefully and implement the most concise solution that changes as little code as possible.

## 环境 & 命令

必须使用 PDM 管理虚拟环境和依赖：

```bash
pdm install              # 安装依赖
pdm run pytest           # 运行所有测试
pdm run pytest tests/test_fan_out.py -v   # 单个测试文件
pdm run pytest tests/test_fan_out.py::test_fan_out_to_executor_types -v  # 单个测试用例
pdm run pytest --cov=src/streamlet        # 带覆盖率
pdm run ruff check src/ tests/            # lint
pdm run ruff format src/ tests/           # 格式化
pdm run mypy src/streamlet/              # 类型检查
pdm run bandit -r src/                    # 安全扫描
pdm build                                 # 构建分发包
```

pytest 默认参数已包含 `-v --tb=short`。

## 项目架构

**Streamlet** 是一个声明式数据流处理框架。用户通过 `@node` 把普通函数变成可组合节点，再通过 fluent 方法链表达业务逻辑：

- `then()`：顺序流水线
- `fan_out_to()`：并行扇出
- `fan_in()` / `fan_out_in()`：聚合并行结果
- `branch_on()`：条件分支
- `repeat()`：重复执行

框架自动处理 async/sync 混合执行、线程/协程并发、Pydantic 校验、依赖注入、ContextVar 隔离和可配置重试。

### 源码结构

| 文件 | 职责 |
|------|------|
| `__init__.py` | 公共 API 重导出，维护 `__all__`（19 个公开符号） |
| `node.py` | `Node` 类、fluent 方法、`@node` 装饰器 |
| `graph.py` | 内部组合类：`Pipeline`、`Parallel`、`Conditional`、`Repeat`、`FanIn` |
| `executor.py` | `SyncExecutor`、`AsyncExecutor`、`ParallelResult`、`FanOutArgs` |
| `context.py` | `BaseFlowContext` DI 容器、`ContextVarProvider`、`custom_validate_call` |
| `retry.py` | `RetryConfig`、`retry_decorator`、指数退避重试 |
| `types.py` | `RepeatInputMode`、`CallArgs`、`call_args` 等公共协议类型 |
| `exceptions.py` | 8 个异常类继承层次 + `retryable` 协议 |
| `py.typed` | PEP 561 类型标记，随包发布给类型检查器使用 |

### 模块依赖方向（单向，无循环）

```
node.py → graph.py → executor.py
   ↓         ↓           ↓
retry.py   types.py   context.py
   ↓
exceptions.py
```

- `types.py` 和 `exceptions.py` 为叶节点，不依赖框架内其他模块
- `retry.py` 仅依赖 `exceptions.py`
- `context.py` 依赖 `exceptions.py` + `retry.py`
- `executor.py` 仅依赖 `context.py`
- `graph.py` 依赖 `executor.py` + `exceptions.py` + `types.py`
- `node.py` 依赖上述全部模块（入口层）

### 架构边界

- `Node` 是用户主要接触的类型；内部 graph 类不作为公共 API 暴露
- `node.py` 负责用户接口和装饰器组合，不承载执行器细节
- `graph.py` 负责组合语义，不直接做线程池或 `asyncio.gather` 底层调度
- `executor.py` 负责执行策略和并行结果包装，不理解业务分支语义
- `context.py` 负责 DI 容器、ContextVar 状态和 Pydantic 校验包装
- `retry.py` 只处理重试策略，不吞掉最终异常
- `__init__.py` 应保持为纯重导出层；新增公共 API 必须同步更新 `__all__`

新增能力时优先沿这些边界放置代码，不要混在一起。

### 核心设计模式

- **组合优于继承**: `Node._func` 持有 Graph 内部类实例，用户永远看不见 Pipeline/Parallel 等
- **双重执行**: `SyncExecutor`（ThreadPoolExecutor 扇出）+ `AsyncExecutor`（asyncio.gather 扇出），`Parallel` 通过 `"thread"` / `"async"` / `"auto"` 选择策略
- **ContextVar 状态隔离**: fan-out 时 `capture_context()` 快照 → 每个 worker 线程/协程 `apply_context()` 恢复浅拷贝，分支间互不污染
- **异常驱动的重试门控**: 异常类通过 `retryable` 类属性声明可重试性，`RetryConfig.should_retry()` 据此决定是否重试
- **FanOutArgs 哨兵协议**: source 返回 `fan_out_args(dict1, dict2)` 时为每个 target 传递独立 kwargs；返回普通值则广播同一参数给所有 target

### 核心实现约定

- `_is_async` 沿组合链向上传播；混合 async/sync 时要保持 `Node.__call__`、`_execute`、`_execute_async` 语义一致
- `Parallel` 支持 `executor="thread" | "async" | "auto"`；`thread` 走 `ThreadPoolExecutor`，`async` 走 `asyncio.gather`，`auto` 根据节点异步性选择
- fan-out 普通返回值广播给所有 target；返回 `fan_out_args(dict1, dict2, ...)` 时每个 dict 作为对应 target 的 kwargs
- `FanOutArgs` 数量必须与 targets 数量一致
- 并行 target 失败包装为 `ParallelResult(success=False)`，不阻断其他 target；同名 target 的并行结果键需经 `_unique_key()` 去重
- `repeat(stop_on_error=True)` 失败时抛 `LoopControlException`；默认记录 warning 并继续
- `repeat()` 默认 `RepeatInputMode.PREVIOUS_RESULT`；普通返回值作为下一轮单个位置参数，`call_args(...)` 显式传递下一轮 `*args/**kwargs`
- `RepeatInputMode.SAME_INPUT` 每轮复用初始调用参数
- `RetryConfig.should_retry()` 优先尊重异常对象或异常类上的 `retryable` 属性
- `@node` 装饰器顺序：校验 → 可选重试 → 按需依赖注入
- Pydantic 输入验证和返回值验证分别抛 `ValidationInputException` 与 `ValidationOutputException`

### 关键代码路径

| 操作 | 执行路径 |
|------|---------|
| `node_func(x)` | `@node` 装饰器包裹层 → `Node.__call__` → `Node._execute` |
| `.then(right)` | 创建 `Pipeline(left=当前, right)` 包装为 `Node` |
| `.fan_out_to(targets)` | 创建 `Parallel(source=当前, targets)` → `SyncExecutor.gather` 或 `AsyncExecutor.agather` |
| `.fan_in()` / `.fan_out_in()` | `FanIn` 聚合并行 → 传入下游 |
| fan-out 上下文隔离 | `capture_context()` → 每个 worker `apply_context(snapshot)` → 执行 → 返回 `ParallelResult` |
| `branch_on({k: node})` | `Conditional` 执行 condition → 用返回值查 `branches` dict → 执行选中分支 |

## 编码规范

- Python 版本要求 `>=3.10`
- 所有函数必须有类型注解，符合 mypy strict 模式
- 格式化和 lint 以 Ruff 为准：88 字符行宽、双引号、空格缩进
- 导入顺序：标准库 → 第三方库 → 项目内部模块
- 日志使用标准 `logging`，logger 名称为 `"streamlet"`
- 命名：函数/变量 `snake_case`，类 `PascalCase`，常量 `UPPER_CASE`

## 测试策略

- 测试节点定义在模块级别，使用 `@node` 装饰器避免 pickle 或执行器场景下不可序列化
- 用户侧行为优先用 `@node` 测试；内部 graph 组合可用 stub 直接测
- 并发相关改动覆盖同步线程池和 async 两条路径
- 上下文相关改动验证线程/协程隔离以及 fan-out 分支互不污染
- 验证、异常、重试和边界条件必须有准确测试，避免只测 happy path
- 测试要 verbose，便于调试
- 不做 mocking，使用真实服务

## 常见风险点

- `asyncio.run()` 不能在已运行的 event loop 中嵌套，改 async 路径要特别小心
- `dependency-injector` 的 provider 会被 deepcopy，`ContextVarProvider.__deepcopy__` 不能破坏注册表
- `FanOutArgs` 数量与 targets 不一致时会报错，错误信息要直接可诊断
- 异常的 `retryable` 协议影响重试决策，新增异常类时要显式考虑
- Node 不支持 pickle 序列化，不要依赖 `ProcessPoolExecutor` 传递 Node 实例
- Graph 内部类绝不对外暴露，仅由 Node fluent 方法内部创建

## 技术约束

- 新依赖需添加到 `pyproject.toml` 并评估必要性
- 异常类继承自 `StreamletException`，用户业务异常继承自 `UserBusinessException`（默认 `retryable=True`）

## ABSOLUTE RULES

- NO PARTIAL IMPLEMENTATION — 不写半成品
- NO SIMPLIFICATION — 不写 "simplified for now" 占位实现
- NO CODE DUPLICATION — 先查已有实现再写新代码
- NO DEAD CODE — 代码要么用要么删
- IMPLEMENT TEST FOR EVERY FUNCTION — 每个函数都要有测试
- NO CHEATER TESTS — 测试要能发现真正的 bug，不能做样子
- NO INCONSISTENT NAMING — 遵循现有命名模式
- NO OVER-ENGINEERING — 简单函数能搞定就别上工厂模式/中间件
- NO MIXED CONCERNS — 不要混入不相关的逻辑
- NO RESOURCE LEAKS — 别忘了关连接、清理 handle
