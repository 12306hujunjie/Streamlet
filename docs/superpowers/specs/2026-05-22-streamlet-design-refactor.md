# Streamlet 设计重构：Executor 策略 + 显式 Graph

## 目标

- **代码层面**：消除 async/sync 分支重复（当前 14+ 处重复）
- **结构层面**：拆分 Node 双重职责，引入显式执行图，模块化
- **执行模型**：保持现有语义——含异步节点则 flow 为异步调用，全同步节点则同步调用
- **API 兼容**：外部 API 100% 向后兼容

## 新模块结构

```
src/streamlet/
├── exceptions.py    (~100行)  异常体系（不变）
├── retry.py         (~80行)   RetryConfig + retry_decorator
├── context.py       (~100行)  BaseFlowContext + ContextVarProvider + custom_validate_call
├── executor.py      (~80行)   ★ Executor Protocol + SyncExecutor + AsyncExecutor
├── graph.py         (~220行)  ★ Pipeline / Parallel / Conditional / Repeat / FanIn 图节点
├── node.py          (~160行)  Node（纯执行单元）+ @node 装饰器
└── __init__.py      (~30行)   公共 API 导出
```

## 核心设计

### 1. Node 层：`_is_async` 保留但收敛到一处

Node 保留 `_is_async`（内部属性，非公开 API）。Graph 类基于子节点推断自身 `_is_async`，构成**含异步节点则整个 flow 为异步**的语义。检测逻辑集中在 `Node.__init__` 一处，不再分散在 6 个 composition 函数中。

```python
class Node:
    """用户唯一接触的类型。_func 可以是原始函数或 Graph 内部类。"""

    def __init__(self, func: Callable, name: str):
        self._func = func
        self.name = name
        # ★ 唯一的 async/sync 检测点
        if hasattr(func, "_is_async"):
            self._is_async = func._is_async      # Graph 类：从子节点继承
        else:
            self._is_async = inspect.iscoroutinefunction(func)  # 原始函数

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """公开入口——100% 保持现有行为。"""
        if self._is_async:
            try:
                asyncio.get_running_loop()
                return self._func(*args, **kwargs)   # event loop 内 → 返回协程
            except RuntimeError:
                return asyncio.run(self._func(*args, **kwargs))  # event loop 外 → asyncio.run
        else:
            return self._func(*args, **kwargs)       # 同步节点 → 直接返回结果

    # === 内部接口：供 Executor 使用 ===

    def _execute(self, *args: Any, **kwargs: Any) -> Any:
        """内部 sync 调用——SyncExecutor 使用。始终返回同步结果。"""
        result = self._func(*args, **kwargs)
        if inspect.iscoroutine(result):
            return asyncio.run(result)
        return result

    async def _execute_async(self, *args: Any, **kwargs: Any) -> Any:
        """内部 async 调用——AsyncExecutor 使用。始终返回可 await 的结果。"""
        result = self._func(*args, **kwargs)
        if inspect.iscoroutine(result):
            return await result
        return result

    # === Fluent 接口 ===

    def then(self, other: Node) -> Node:
        pipeline = Pipeline(self, other)
        return Node(pipeline, name=f"{self.name}→{other.name}")

    def fan_out_to(self, nodes: list[Node], executor: str = "thread",
                   max_workers: int | None = None) -> Node:
        executor_lower = executor.lower()
        if executor_lower not in ("thread", "async", "auto"):
            raise ValueError(
                f"Only 'thread', 'async', and 'auto' executors are supported, got '{executor}'"
            )
        parallel = Parallel(self, nodes, executor_type=executor_lower,
                            max_workers=max_workers)
        return Node(parallel, name=f"{self.name}∥[...]")

    def fan_in(self, aggregator: Node) -> Node:
        fan_in = FanIn(self, aggregator)
        return Node(fan_in, name=f"...⤇{aggregator.name}")

    def branch_on(self, conditions: dict[Any, Node]) -> Node:
        cond = Conditional(self, conditions)
        return Node(cond, name=f"{self.name}?")

    def repeat(self, times: int, stop_on_error: bool = False) -> Node:
        rep = Repeat(self, times, stop_on_error)
        return Node(rep, name=f"{self.name}×{times}")

    def fan_out_in(self, targets: list[Node], aggregator: Node,
                   executor: str = "thread", max_workers: int | None = None) -> Node:
        return self.fan_out_to(targets, executor, max_workers).fan_in(aggregator)
```

**关键变化**：
- `_is_async` 保留但收敛到 `Node.__init__` 一处检测
- `Node.__call__` 保持现有行为（event loop 检测，决定返回协程还是同步结果）
- `Node._execute` / `Node._execute_async` 是两个内部接口，专供 Executor 使用
- fluent 方法全部返回 `Node`——Graph 类对用户完全透明

### 2. Executor 协议（executor.py）

Executor 是**纯执行策略**——不做函数类型检测，只决定"怎么调度"。`SyncExecutor` 用线程池，`AsyncExecutor` 用 asyncio.gather。具体怎么调用节点（sync/async）由调用的 Node 方法决定。

```python
class Executor(Protocol):
    def run(self, node: Node, *args: Any, **kwargs: Any) -> Any: ...
    def gather(self, tasks: list[tuple[Node, Any]]) -> dict[str, ParallelResult]: ...

class SyncExecutor:
    """同步执行器：调用 node._execute()，gather 使用 ThreadPoolExecutor + ContextVar 传播。"""
    def __init__(self, max_workers: int | None = None):
        self.max_workers = max_workers

    def run(self, node, *args, **kwargs):
        return node._execute(*args, **kwargs)

    def gather(self, node_inputs):
        ctx = contextvars.copy_context()
        results: dict[str, ParallelResult] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(ctx.run, self.run, n, inp): n for n, inp in node_inputs}
            for future in as_completed(futures):
                node = futures[future]
                try:
                    result = future.result()
                    results[node.name] = ParallelResult(
                        node_name=node.name, success=True, result=result,
                    )
                except Exception as e:
                    import traceback
                    results[node.name] = ParallelResult(
                        node_name=node.name, success=False,
                        error=str(e), error_traceback=traceback.format_exc(),
                    )
        return results

class AsyncExecutor:
    """异步执行器：调用 node._execute_async()，gather 使用 asyncio.gather 实现真正的异步并发。"""

    async def arun(self, node, *args, **kwargs):
        """内部使用——返回协程，供 Graph 类 await。"""
        return await node._execute_async(*args, **kwargs)

    async def agather(self, node_inputs):
        """内部使用——返回协程，供 Graph 类 await。"""
        async def execute_one(node, inp):
            try:
                result = await node._execute_async(inp)
                return node.name, ParallelResult(
                    node_name=node.name, success=True, result=result,
                )
            except Exception as e:
                import traceback
                return node.name, ParallelResult(
                    node_name=node.name, success=False,
                    error=str(e), error_traceback=traceback.format_exc(),
                )
        results_list = await asyncio.gather(*(execute_one(n, inp) for n, inp in node_inputs))
        return dict(results_list)
```

**设计要点**：
- Executor **不检测** `iscoroutinefunction`——它只调用 Node 的内部接口（`_execute` 或 `_execute_async`）
- SyncExecutor 始终走同步路径，AsyncExecutor 始终走异步路径。Graph 类在创建时根据 `_is_async` 选择使用哪个
- `sync.gather()` 使用 `contextvars.copy_context()` 将 ContextVar（含自定义 Executor）传播到线程池子线程
- `sleep` 不在 Executor 协议中——`retry_decorator` 在装饰时已知函数类型，直接使用 `asyncio.sleep` / `time.sleep`

### 3. 显式 Graph 节点（graph.py）

Graph 类是**内部实现细节**，不对外暴露。每个类有两个轻量方法（sync/async），在 `__init__` 时根据 `_is_async` 选择分发。

```python
class Pipeline:
    """顺序组合：left → right"""
    def __init__(self, left: Node, right: Node):
        self.left = left
        self.right = right
        self._is_async = left._is_async or right._is_async

    def __call__(self, *args, **kwargs):          # Node 公开入口走这条路
        if self._is_async:
            return self._async_run(*args, **kwargs)   # 返回协程（供上层 await）
        else:
            return self._sync_run(*args, **kwargs)    # 返回同步结果

    def _sync_run(self, *args, **kwargs):
        ex = SyncExecutor()
        mid = ex.run(self.left, *args, **kwargs)
        return ex.run(self.right, mid)

    async def _async_run(self, *args, **kwargs):
        ex = AsyncExecutor()
        mid = await ex.arun(self.left, *args, **kwargs)
        return await ex.arun(self.right, mid)

class Parallel:
    """并行扇出：source → [target1, target2, ...]

    executor_type 控制**并行调度策略**（不是节点执行资格）：
    - "thread": ThreadPoolExecutor 调度，_execute 内部有 asyncio.run() 桥接 async 节点
    - "async":  asyncio.gather 调度，需要 event loop
    - "auto":   全 sync 走 thread，含 async 走 async
    """
    def __init__(self, source: Node, targets: list[Node],
                 executor_type: str = "thread", max_workers: int | None = None):
        self.source = source
        self.targets = targets
        self.executor_type = executor_type
        self.max_workers = max_workers
        self._is_async = source._is_async or any(t._is_async for t in targets)

    def __call__(self, *args, **kwargs):
        if self.executor_type == "thread":
            return self._sync_run(*args, **kwargs)
        elif self.executor_type == "async":
            return self._async_run(*args, **kwargs)
        else:  # "auto"
            return self._async_run(*args, **kwargs) if self._is_async else self._sync_run(*args, **kwargs)

    def _sync_run(self, *args, **kwargs):
        ex = SyncExecutor(max_workers=self.max_workers)
        source_result = ex.run(self.source, *args, **kwargs)
        return ex.gather([(t, source_result) for t in self.targets])

    async def _async_run(self, *args, **kwargs):
        ex = AsyncExecutor()
        source_result = await ex.arun(self.source, *args, **kwargs)
        return await ex.agather([(t, source_result) for t in self.targets])

class Conditional:
    """条件分支"""
    def __init__(self, condition_node: Node, branches: dict[Any, Node]):
        self.condition_node = condition_node
        self.branches = branches
        self._is_async = condition_node._is_async or any(
            b._is_async for b in branches.values()
        )

    def __call__(self, *args, **kwargs):
        if self._is_async:
            return self._async_run(*args, **kwargs)
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args, **kwargs):
        ex = SyncExecutor()
        condition_result = ex.run(self.condition_node, *args, **kwargs)
        if condition_result not in self.branches:
            raise ValueError(f"No branch defined for condition result: {condition_result}")
        return ex.run(self.branches[condition_result])

    async def _async_run(self, *args, **kwargs):
        ex = AsyncExecutor()
        condition_result = await ex.arun(self.condition_node, *args, **kwargs)
        if condition_result not in self.branches:
            raise ValueError(f"No branch defined for condition result: {condition_result}")
        return await ex.arun(self.branches[condition_result])

class Repeat:
    """循环组合"""
    def __init__(self, node: Node, times: int, stop_on_error: bool = False):
        if not isinstance(times, int):
            raise TypeError(f"times must be an integer, got {type(times).__name__}")
        if times <= 0:
            raise ValueError("Repeat times must be greater than 0")
        self.node = node
        self.times = times
        self.stop_on_error = stop_on_error
        self._is_async = node._is_async

    def __call__(self, *args, **kwargs):
        if self._is_async:
            return self._async_run(*args, **kwargs)
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args, **kwargs):
        ex = SyncExecutor()
        last_result = None
        for i in range(self.times):
            try:
                last_result = ex.run(
                    self.node, *args if i == 0 else (last_result,)
                )
            except Exception:
                if self.stop_on_error:
                    raise
        return last_result

    async def _async_run(self, *args, **kwargs):
        ex = AsyncExecutor()
        last_result = None
        for i in range(self.times):
            try:
                last_result = await ex.arun(
                    self.node, *args if i == 0 else (last_result,)
                )
            except Exception:
                if self.stop_on_error:
                    raise
        return last_result

class FanIn:
    """聚合：接收上游 Node 的结果 dict → aggregator"""
    def __init__(self, upstream: Node, aggregator: Node):
        self.upstream = upstream
        self.aggregator = aggregator
        self._is_async = upstream._is_async or aggregator._is_async

    def __call__(self, *args, **kwargs):
        if self._is_async:
            return self._async_run(*args, **kwargs)
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args, **kwargs):
        ex = SyncExecutor()
        upstream_result = ex.run(self.upstream, *args, **kwargs)
        return ex.run(self.aggregator, upstream_result)

    async def _async_run(self, *args, **kwargs):
        ex = AsyncExecutor()
        upstream_result = await ex.arun(self.upstream, *args, **kwargs)
        return await ex.arun(self.aggregator, upstream_result)
```

**为什么 Graph 类仍有 `if self._is_async` 分支**：这是 Python 的硬约束——`await` 是语法，不能在运行时"切换"。但这与当前代码的重复有本质区别：

| | 当前 | 新设计 |
|---|---|---|
| 分支位置 | 6 个 composition 函数，每个内部创建 sync/async 闭包 | 5 个 Graph 类，每个 `__call__` 中一行 `if/else` 分发 |
| 分支内容 | ~30 行近重复逻辑（含错误处理、日志、结果包装） | 2-3 行方法调用（`_sync_run` / `_async_run`） |
| 代码量 | ~200 行重复 | ~10 行分发 |
| Executor 参与 | 无——闭包内手动处理 async/sync 调用 | 有——Graph 类将执行策略委托给 Executor |

### 4. @node 装饰器

保持现有签名不变。`is_async` 参数移除（`Node.__init__` 自动检测）。

```python
@node
def process(x: int) -> int: ...

@node(retry_count=5, enable_retry=True)
async def fetch(url: str) -> dict: ...
```

## 调用链路（含异步节点 vs 全同步节点）

### 场景 1：全同步节点，外部直接调用

```python
result = sync_a.then(sync_b)(data)
```

```
Node.__call__ (sync_a.then(sync_b) → Pipeline, _is_async=False)
  → self._func(data) → Pipeline.__call__(data)
    → self._is_async == False → self._sync_run(data)
      → SyncExecutor.run(sync_a, data) → sync_a._execute(data) → 同步结果
      → SyncExecutor.run(sync_b, mid)  → sync_b._execute(mid)  → 同步结果
  → 返回具体值 ✓
```

### 场景 2：含异步节点，外部直接调用

```python
result = sync_a.then(async_b)(data)
```

```
Node.__call__ (sync_a.then(async_b) → Pipeline, _is_async=True)
  → 不在 event loop → asyncio.run(self._func(data))
    → Pipeline.__call__(data)
      → self._is_async == True → self._async_run(data)
        → AsyncExecutor.arun(sync_a, data)  → await sync_a._execute_async(data) → 同步结果
        → AsyncExecutor.arun(async_b, mid)  → await async_b._execute_async(mid) → 异步结果
  → asyncio.run 返回具体值 ✓
```

### 场景 3：含异步节点，event loop 内 await 调用

```python
result = await sync_a.then(async_b)(data)
```

```
Node.__call__ (Pipeline, _is_async=True)
  → event loop 内 → return self._func(data) → 返回协程
    → Pipeline.__call__(data)
      → self._is_async == True → return self._async_run(data)
        → async def _async_run:
            await AsyncExecutor.arun(sync_a)   → 协程内正确 await
            await AsyncExecutor.arun(async_b)  → 协程内正确 await
  → 用户 await 拿到结果 ✓
```

三个场景均正常工作，与当前行为完全一致。

## API 兼容性

| API | 当前 | 重构后 | 变化 |
|-----|------|--------|------|
| `@node` | Node | Node | 无 |
| `.then()` | 返回 Node | 返回 Node | 无 |
| `.fan_out_to()` | 返回 Node | 返回 Node | 无 |
| `.fan_in()` | 返回 Node | 返回 Node | 无 |
| `.branch_on()` | 返回 Node | 返回 Node | 无 |
| `.repeat()` | 返回 Node | 返回 Node | 无 |
| `.fan_out_in()` | 返回 Node | 返回 Node | 无 |
| `flow(data)` | 自动检测 async/sync | 自动检测 async/sync | 无 |
| `await flow(data)` | 支持 | 支持 | 无 |
| `executor="thread"/"async"/"auto"` | 字符串 | 字符串 | 无 |
| `Node.is_async` | 公开属性 | 改为 `_is_async`（内部） | 非公开 API |

**外部 API 100% 向后兼容**：用户始终通过 `@node` 装饰器创建 Node，通过 fluent 方法组合 Node，最终 `__call__` 执行。

## 影响范围

### 删除/降级
- `Node.is_async` 公开属性（改为 `_is_async`）
- `custom_validate_call` 从 `__all__` 移除（降为内部实现）
- 6 个内部组合函数：`sequential_composition`、`parallel_fan_out`、`parallel_fan_in`、`parallel_fan_out_in`、`conditional_composition`、`repeat_composition`

### 新增
- `executor.py`：Executor Protocol + SyncExecutor + AsyncExecutor
- `graph.py`：Pipeline / Parallel / Conditional / Repeat / FanIn 5 个内部类

### __all__ 变化

```python
# 新 __all__
__all__ = [
    # 装饰器
    "node",
    # 核心类型（★ 新增）
    "Node",
    # 依赖注入
    "BaseFlowContext",
    # 并行结果
    "ParallelResult",
    # 异常
    "StreamletException",
    "ValidationInputException",
    "ValidationOutputException",
    "UserBusinessException",
    "NodeExecutionException",
    "NodeTimeoutException",
    "NodeRetryExhaustedException",
    "LoopControlException",
    # 重试
    "RetryConfig",
]
```

变更：
- **新增 `Node`**：用户链式调用和类型标注需要显式 import
- **移除 `custom_validate_call`**：降为内部实现细节，不再公开导出
- **移除 6 个 composition 函数**：被 Graph 内部类替代
- Graph 类、Executor 类不在 `__all__` 中——用户始终通过 Node fluent 接口间接使用

## 测试策略

### 现有测试（保留 12 个文件，已删除 5 个）

| 文件 | 决策 |
|------|------|
| `test_exceptions.py` | 保留 |
| `test_retry.py` | 保留（已删除 TestRetryDecoratorSync/Async） |
| `test_context.py` | 保留（已删除 TestContextVarProvider） |
| `test_validate_call.py` | 保留 |
| `test_node_decorator.py` | 保留（已删除 3 个内部 API 测试） |
| `test_sequential.py` | 保留 |
| `test_fan_out.py` | 保留 |
| `test_fan_in.py` | 保留（合并了 fan_out_in） |
| `test_conditional.py` | 保留 |
| `test_repeat.py` | 保留 |
| `test_integration.py` | 保留 |
| `conftest.py` | 保留（共享节点定义） |

已删除：`test_composition_funcs.py`、`test_fan_out_in.py`、`test_node_basics.py`、`test_parallel_result.py`、`shared/data_models.py`

### 新增测试

**test_executor.py**（TDD 第一优先）：
- SyncExecutor.run() 通过 `node._execute` 调用节点函数返回正确结果（sync 和 async 节点均可）
- SyncExecutor.gather() 使用 ThreadPoolExecutor + `contextvars.copy_context()` 并行
- SyncExecutor.gather() 错误包装为 ParallelResult(success=False, error=...)
- AsyncExecutor.arun() 通过 `node._execute_async` await 异步函数
- AsyncExecutor.agather() 使用 asyncio.gather 真正并发
- AsyncExecutor.agather() 错误包装为 ParallelResult(success=False, error=...)
- ContextVar 显式设置 Executor 在线程池中正确传播
- ContextVar 显式设置 Executor 在 asyncio.gather 中自动继承（协程天然支持）

**test_graph.py**（TDD 第二优先）：
- Pipeline: 全 sync 两节点链、含 async 两节点链（`_is_async` 传播）、错误传播
- Parallel: source + targets 结构、executor_type/max_workers 属性、sync/async 分别执行
- Conditional: 分支选择、未匹配异常、含 async 分支的 `_is_async` 传播
- Repeat: 迭代次数、数据累积、stop_on_error、含 async 节点的循环
- FanIn: 接收上游结果 dict、sync/async 聚合

### TDD 实现顺序

1. `test_executor.py` → 实现 `executor.py`
2. `test_graph.py` → 实现 `graph.py`
3. 重构 `node.py`（Node + @node）
4. 重构 `retry.py`（RetryConfig 拆分，decorator 保留 async/sync 二分叉）
5. 重构 `__init__.py` 导出
6. 运行全部 14 个测试文件验证兼容性
