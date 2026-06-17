# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 核心原则

- **理解优于实现**: 深入分析现有代码结构和设计意图后再修改
- **渐进优于激进**: 小步骤改进，每个变更都可验证和回滚
- **证据优于假设**: 所有决策基于实际测试和分析，不依赖猜测
- **简单优于复杂**: 优先选择最简单有效的解决方案
- **显式胜于隐式**: fluent interface设计中明确表达数据流和转换逻辑
- **可读性很重要**: 复杂异步流程必须保持清晰的代码结构和意图表达

## 项目架构

### 模块划分（共 7 个源文件，~1040 行）

| 模块 | 行数 | 职责 |
|---|---|---|
| `src/streamlet/__init__.py` | 37 | 公共 API 重导出中枢（14 个公开符号） |
| `src/streamlet/node.py` | 203 | `Node` 类 + `@node` 装饰器 |
| `src/streamlet/graph.py` | 217 | 5 个内部组合类（Pipeline/Parallel/Conditional/Repeat/FanIn） |
| `src/streamlet/executor.py` | 162 | `SyncExecutor` / `AsyncExecutor` + `ParallelResult` |
| `src/streamlet/context.py` | 154 | `BaseFlowContext` DI 容器 + `custom_validate_call` |
| `src/streamlet/retry.py` | 153 | `RetryConfig` + `retry_decorator` |
| `src/streamlet/exceptions.py` | 117 | 8 个异常类 |

### 核心概念

- **Node类**: 执行图的基本单元，支持 fluent interface 链式调用，包装原始函数或内部 Graph 对象
- **@node装饰器**: 构建装饰器链 `validate → retry → 按需 di_inject`，通过 `functools.reduce` 组合
- **流式接口**: `.then()`, `.fan_out_to()`, `.fan_in()`, `.branch_on()`, `.repeat()` 创建内部 Graph 对象后重新包装为 Node
- **5 个内部组合类**: 用户不直接实例化，由 Node fluent 方法创建。每个类实现 `_sync_run` / `_async_run`，通过 `_is_async` 标志分发
- **双执行器模式**: `SyncExecutor`（ThreadPoolExecutor）+ `AsyncExecutor`（asyncio.gather），共享 `run/gather` 概念接口
- **智能异步系统**: `_is_async` 沿组合链向上传播（OR 逻辑），自动选择同步/异步执行路径
- **依赖注入**: `BaseFlowContext` 提供单一 `context` provider（`ContextVarProvider[dict]`），默认线程/协程隔离；fan-out 分支从父上下文复制浅拷贝
- **重试机制**: `RetryConfig` 配置指数退避，通过 `retryable` 类属性协议判断异常是否可重试

### 核心类和方法发现指南
- `node.py` — `class Node` fluent 方法：`then`, `fan_out_to`, `fan_in`, `branch_on`, `repeat`, `fan_out_in`
- `graph.py` — 5 个内部组合类的 `_sync_run` / `_async_run` 实现
- `executor.py` — `SyncExecutor` / `AsyncExecutor` + `ParallelResult` 数据类
- `context.py` — `BaseFlowContext` DI 容器 + `custom_validate_call` + `ContextVarProvider`
- `retry.py` — `RetryConfig` + `retry_decorator` + `_get_func_name`
- `exceptions.py` — 8 个异常类的继承层次和 `retryable` 属性协议

## 开发命令

### 环境管理
```bash
# 使用pdm管理虚拟环境（必须）
pdm install               # 安装所有依赖
pdm install --dev        # 安装开发依赖
pdm list                 # 查看依赖树
```

### 测试命令
```bash
# 运行所有测试
pdm run python -m pytest

# 运行特定测试文件
pdm run python -m pytest tests/test_sequential.py -v
pdm run python -m pytest tests/test_fan_out.py -v
pdm run python -m pytest tests/test_graph.py -v

# 运行特定测试函数
pdm run python -m pytest tests/test_fan_out.py::test_fan_out_to_executor_types -v

# 带覆盖率报告
pdm run python -m pytest --cov=src/streamlet --cov-report=html

# 并行测试（如果需要）
pdm run python -m pytest -n auto
```

### 代码质量检查
```bash
# Linting (Ruff配置在pyproject.toml中)
pdm run ruff check src/ tests/
pdm run ruff format src/ tests/

# 类型检查 (MyPy配置在pyproject.toml中)
pdm run mypy src/streamlet/

# 安全扫描 (Bandit配置排除tests目录)
pdm run bandit -r src/
```

## 技术约束

### 环境要求
- **必须使用PDM**: 不使用pip、conda或全局Python环境
- **Python 3.10+**: 项目需要现代Python特性
- **依赖管理**: 新依赖需要添加到pyproject.toml并评估必要性

### 代码风格
- **Ruff格式化**: 88字符行长，双引号，空格缩进
- **类型注解**: 所有函数必须有类型注解（MyPy strict模式）
- **Pydantic验证**: 使用Pydantic BaseModel进行数据验证
- **日志记录**: 使用标准logging库，logger名称为"streamlet"
- **命名约定**: 函数和变量使用snake_case，类使用PascalCase，常量使用UPPER_CASE
- **文档字符串**: 使用简洁的docstring描述节点功能和参数，遵循项目现有风格
- **导入组织**: 标准库 → 第三方库 → 项目内部导入，使用绝对导入路径

### 架构约束
- **模块职责单一**: 每个模块一个清晰职责（node/graph/executor/context/retry/exceptions），`__init__.py` 纯重导出
- **内部/外部分离**: Graph 内部类（Pipeline/Parallel/Conditional/Repeat/FanIn）不对外暴露，由 Node fluent 方法创建
- **线程/协程隔离**: `ContextVarProvider` + `capture_context()` / `apply_context()`；fan-out 分支复制父 `context`，分支写入互不污染
- **可序列化**: 所有 Node 必须支持 pickle 序列化（用于进程池）
- **依赖注入**: 使用 dependency-injector 容器管理状态，`@node` 在签名包含 `Provide[...]` / `Provider[...]` 默认值或 `Annotated[..., Provide[...]]` 元数据时按需注入
- **组合层次**: Graph 组合类处理执行逻辑，Node 类提供用户接口，@node 装饰器仅用于用户业务节点
- **retryable 属性协议**: 异常类通过 `retryable` 类属性声明可重试性，`RetryConfig.should_retry()` 优先检查此属性

## 测试原则

### 测试文件组织（15 个测试文件 + conftest.py，~2110 行）

| 测试文件 | 测试目标 |
|---|---|
| `tests/conftest.py` | 共享 fixture（`container`）+ 可复用 `@node` 测试节点 |
| `tests/test_node_decorator.py` | @node 装饰器调用模式、async/sync、DI、类型验证 |
| `tests/test_graph.py` | 5 个内部 Graph 类的单元测试（使用 StubNode） |
| `tests/test_executor.py` | SyncExecutor/AsyncExecutor 的 run/gather、ContextVar 传播 |
| `tests/test_retry.py` | RetryConfig 配置、重试/指数退避/异常判断 |
| `tests/test_exceptions.py` | 8 个异常类的继承层次和 retryable 属性 |
| `tests/test_context.py` | BaseFlowContext provider 和线程/协程隔离 |
| `tests/test_async_state_isolation.py` | async fan-out 分支的 context 继承与隔离 |
| `tests/test_validate_call.py` | custom_validate_call 输入/输出验证 |
| `tests/test_sequential.py` | Node.then() 链式调用及 async/sync 混合 |
| `tests/test_fan_out.py` | Node.fan_out_to() 并行扇出 |
| `tests/test_fan_in.py` | Node.fan_in() 聚合 + fan_out_in() |
| `tests/test_conditional.py` | Node.branch_on() 条件分支 |
| `tests/test_repeat.py` | Node.repeat() 循环执行 |
| `tests/test_integration.py` | E2E 工作流：ETL、扇出扇入、条件路由、线程安全、错误恢复 |

### 核心测试约束
- 测试节点必须定义在模块级别以支持 pickle 序列化
- 使用 `@node` 装饰器而非直接实例化 Node 类
- `tests/test_graph.py` 使用 `StubNode` 直接测试内部 Graph 类
- 每个测试使用独立的依赖注入容器实例

## 调试和问题排查

### 常见问题
1. **Pickle序列化错误**: 确保节点函数定义在模块级别，不使用lambda
2. **依赖注入失败**: 检查容器wire配置和Provide注解
3. **并发竞争条件**: 验证 `ContextVarProvider` 状态隔离和 fan-out 分支浅拷贝
4. **测试超时**: 检查并行执行器的max_workers配置

### 调试技巧
- 启用详细日志: `logging.getLogger("streamlet").setLevel(logging.DEBUG)`
- 检查并行结果: 使用`ParallelResult`数据类分析执行状态
- 状态检查: 通过 `BaseFlowContext.context` 访问当前执行上下文

## 数据流处理特殊考虑

### 性能优化
- **线程vs进程**: 默认使用ThreadPoolExecutor，CPU密集任务考虑ProcessPoolExecutor
- **状态管理**: 避免大对象在线程间传递，使用引用或标识符
- **内存效率**: 大数据流使用生成器模式，避免全量加载
- **数据结构选择**: 使用`collections.deque`用于队列操作，`set`用于去重和成员检查
- **惰性求值**: 优先使用生成器表达式而非列表推导，延迟计算直到真正需要
- **缓存策略**: 使用`functools.lru_cache`缓存计算结果，避免重复计算
- **内置函数优化**: 优先使用内置函数如`map()`, `filter()`, `any()`, `all()`而非手写循环

### 错误恢复
- **节点级重试**: `@node(enable_retry=True)` 使用 `RetryConfig` 和异常 `retryable` 属性判断是否重试
- **并行错误隔离**: fan-out target 失败会包装为 `ParallelResult(success=False)`，不阻断其它 target
- **循环错误策略**: `repeat(stop_on_error=True)` 抛出 `LoopControlException`；默认记录警告并继续使用上一次成功结果

### 监控和观测
- **执行跟踪**: 记录每个节点的执行时间和状态
- **并行可视化**: 使用ParallelResult分析并发执行效果
- **依赖注入诊断**: 验证容器配置和服务注册状态

---

## Claude Code 开发指导原则

> Think carefully and implement the most concise solution that changes as little code as possible.

## USE SUB-AGENTS FOR CONTEXT OPTIMIZATION

### 1. Always use the file-analyzer sub-agent when asked to read files.
The file-analyzer agent is an expert in extracting and summarizing critical information from files, particularly log files and verbose outputs. It provides concise, actionable summaries that preserve essential information while dramatically reducing context usage.

### 2. Always use the code-analyzer sub-agent when asked to search code, analyze code, research bugs, or trace logic flow.

The code-analyzer agent is an expert in code analysis, logic tracing, and vulnerability detection. It provides concise, actionable summaries that preserve essential information while dramatically reducing context usage.

### 3. Always use the test-runner sub-agent to run tests and analyze the test results.

Using the test-runner agent ensures:

- Full test output is captured for debugging
- Main conversation stays clean and focused
- Context usage is optimized
- All issues are properly surfaced
- No approval dialogs interrupt the workflow

## Philosophy

### Error Handling

- **Fail fast** for critical configuration (missing text model)
- **Log and continue** for optional features (extraction model)
- **Graceful degradation** when external services unavailable
- **User-friendly messages** through resilience layer

### Testing

- Always use the test-runner agent to execute tests.
- Do not use mock services for anything ever.
- Do not move on to the next test until the current test is complete.
- If the test fails, consider checking if the test is structured correctly before deciding we need to refactor the codebase.
- Tests to be verbose so we can use them for debugging.

## Tone and Behavior

- Criticism is welcome. Please tell me when I am wrong or mistaken, or even when you think I might be wrong or mistaken.
- Please tell me if there is a better approach than the one I am taking.
- Please tell me if there is a relevant standard or convention that I appear to be unaware of.
- Be skeptical.
- Be concise.
- Short summaries are OK, but don't give an extended breakdown unless we are working through the details of a plan.
- Do not flatter, and do not give compliments unless I am specifically asking for your judgement.
- Occasional pleasantries are fine.
- Feel free to ask many questions. If you are in doubt of my intent, don't guess. Ask.

## ABSOLUTE RULES:

- NO PARTIAL IMPLEMENTATION
- NO SIMPLIFICATION : no "//This is simplified stuff for now, complete implementation would blablabla"
- NO CODE DUPLICATION : check existing codebase to reuse functions and constants Read files before writing new functions. Use common sense function name to find them easily.
- NO DEAD CODE : either use or delete from codebase completely
- IMPLEMENT TEST FOR EVERY FUNCTIONS
- NO CHEATER TESTS : test must be accurate, reflect real usage and be designed to reveal flaws. No useless tests! Design tests to be verbose so we can use them for debuging.
- NO INCONSISTENT NAMING - read existing codebase naming patterns.
- NO OVER-ENGINEERING - Don't add unnecessary abstractions, factory patterns, or middleware when simple functions would work. Don't think "enterprise" when you need "working"
- NO MIXED CONCERNS - Don't put validation logic inside API handlers, database queries inside UI components, etc. instead of proper separation
- NO RESOURCE LEAKS - Don't forget to close database connections, clear timeouts, remove event listeners, or clean up file handles

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
