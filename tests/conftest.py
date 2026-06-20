"""pytest 共享 fixtures 与测试节点。

各测试文件通过 ``from tests.conftest import ...`` 复用这些节点，
避免重复定义语义相同的 @node 装饰函数。
"""

import pytest

from streamlet import BaseFlowContext, node

# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def container():
    """函数级别的依赖注入容器。"""
    return BaseFlowContext()


# ============================================================
# 基础算术节点（then / sequential 测试复用）
# ============================================================


@node
def double(x: int) -> int:
    return x * 2


@node
def add_ten(x: int) -> int:
    return x + 10


@node
def to_string(x: int) -> str:
    return f"result:{x}"


# ============================================================
# 并行处理节点（fan_out / fan_in 测试复用）
# ============================================================


@node
def source_data(x: int) -> dict:
    return {"value": x}


@node
def multiply(data: dict) -> int:
    return data["value"] * 2


@node
def add_five(data: dict) -> int:
    return data["value"] + 5


@node
def square(data: dict) -> int:
    return data["value"] ** 2


@node
def aggregate_sum(results: dict) -> dict:
    successful = [r.result for r in results.values() if r.success]
    return {"total": sum(successful), "count": len(successful)}


# ============================================================
# 异步节点
# ============================================================


@node
async def async_double(x: int) -> int:
    return x * 2


@node
async def async_add_ten(x: int) -> int:
    return x + 10


# ============================================================
# Repeat / 循环节点
# ============================================================


@node
def increment(data: dict) -> dict:
    return {"value": data.get("value", 0) + 1}
