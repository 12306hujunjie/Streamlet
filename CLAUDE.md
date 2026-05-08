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

### 核心概念
Streamlet是一个智能异步数据处理工作流框架，核心架构基于：

- **Node类**: 执行图的基本单元，支持fluent interface链式调用和智能异步/同步混合执行
- **@node装饰器**: 智能节点包装器，自动处理异步/同步兼容性、重试机制和依赖注入
- **流式接口**: `.then()`, `.fan_out_to()`, `.fan_in()`, `.branch_on()`, `.repeat()` 方法支持任意async/sync混合
- **智能异步系统**: 框架自动检测async/sync函数并选择正确的执行策略，无需开发者显式处理
- **并行处理**: 基于ThreadPoolExecutor的扇出/扇入模式，支持混合异步执行
- **依赖注入**: 集成dependency-injector，使用BaseFlowContext进行线程安全状态管理
- **重试机制**: 可配置的指数退避重试，支持异步和同步节点

### 关键文件结构
- `src/aetherflow/__init__.py`: 单一模块包含所有核心功能（~1000行）
- `tests/`: 全面的测试套件，使用@node装饰器模式避免pickle问题
- `tests/utils/node_factory.py`: 标准化测试节点定义
- `tests/shared/`: 共享数据模型和测试常量

### 核心类和方法发现指南
- 使用 `Grep` 搜索 `class Node` 了解Node类的完整定义
- 搜索 `def then|def fan_out|def fan_in|def branch_on|def repeat` 了解fluent interface方法
- 查看 `@node` 装饰器定义了解节点包装机制
- 搜索 `BaseFlowContext` 了解依赖注入容器结构

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
pdm run python -m pytest tests/test_then_core.py -v
pdm run python -m pytest tests/test_fan_primitives.py -v
pdm run python -m pytest tests/test_conditional_composition.py -v

# 运行特定测试函数
pdm run python -m pytest tests/test_fan_primitives.py::test_fan_out_to_executor_types -v

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
pdm run mypy src/aetherflow/

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
- **日志记录**: 使用标准logging库，logger名称为"aetherflow"
- **命名约定**: 函数和变量使用snake_case，类使用PascalCase，常量使用UPPER_CASE
- **文档字符串**: 使用简洁的docstring描述节点功能和参数，遵循项目现有风格
- **导入组织**: 标准库 → 第三方库 → 项目内部导入，使用绝对导入路径

### 架构约束
- **单模块设计**: 核心功能集中在`__init__.py`中，避免过度拆分
- **线程安全**: 使用ThreadLocalSingleton模式进行状态隔离
- **可序列化**: 所有Node必须支持pickle序列化（用于进程池）
- **依赖注入**: 使用dependency-injector容器管理状态
- **组合层次**: composition函数使用Node类，@node装饰器仅用于用户业务节点

## 测试原则

### 测试结构
- **使用@node装饰器**: 测试节点定义在模块级别，支持pickle序列化
- **共享基础设施**: 使用`tests/utils/node_factory.py`的标准化节点
- **数据模型**: 使用`tests/shared/data_models.py`的Pydantic模型
- **隔离性**: 每个测试使用独立的依赖注入容器

### 测试覆盖
- **核心原语测试**: then、fan_out、fan_in、branch_on、repeat功能
- **并发安全测试**: 线程池和进程池执行器的正确性
- **错误处理测试**: 重试机制、异常传播、超时处理
- **依赖注入测试**: 状态管理和线程隔离

### 测试开发指南

**探索现有模式**：
- 使用 `Grep` 工具搜索 `@node` 装饰器了解节点定义模式
- 查看 `tests/utils/` 目录了解当前可用的测试工具和节点工厂
- 参考 `tests/shared/` 目录了解数据模型和测试常量
- 检查现有测试文件学习fluent interface链式调用模式

**核心测试约束**：
- 测试节点必须定义在模块级别以支持pickle序列化
- 使用 `@node` 装饰器而不是直接实例化Node类
- 每个测试应使用独立的依赖注入容器实例
- 异步/同步混合测试需要正确的await处理

**发现测试工具**：
- 搜索 `tests/utils/` 目录了解验证工具
- 查看 `ParallelResult` 相关用法了解并行处理验证
- 检查现有测试的container setup模式

## 调试和问题排查

### 常见问题
1. **Pickle序列化错误**: 确保节点函数定义在模块级别，不使用lambda
2. **依赖注入失败**: 检查容器wire配置和Provide注解
3. **并发竞争条件**: 验证ThreadLocalSingleton状态隔离
4. **测试超时**: 检查并行执行器的max_workers配置

### 调试技巧
- 启用详细日志: `logging.getLogger("aetherflow").setLevel(logging.DEBUG)`
- 检查并行结果: 使用`ParallelResult`数据类分析执行状态
- 状态检查: 通过依赖注入访问线程本地状态

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
- **分层重试**: Node级别重试 + 组合级别重试
- **异常分类**: 区分瞬时异常(TRANSIENT_EXCEPTIONS)和永久异常(PERMANENT_EXCEPTIONS)
- **状态回滚**: 失败时的状态清理和恢复机制

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
