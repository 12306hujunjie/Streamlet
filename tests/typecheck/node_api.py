"""Static type contract for the public ``node`` overloads."""

from collections.abc import Callable
from typing import Any

from typing_extensions import assert_type

from streamlet import Node, node


def increment(value: int) -> int:
    return value + 1


@node
def bare_decorator(value: int) -> int:
    return value + 1


@node()
def empty_factory_decorator(value: int) -> int:
    return value + 1


@node(name="configured")
def configured_decorator(value: int) -> int:
    return value + 1


assert_type(bare_decorator, Node)
assert_type(empty_factory_decorator, Node)
assert_type(configured_decorator, Node)
assert_type(node(increment), Node)
assert_type(node(increment, name="direct"), Node)
assert_type(node(), Callable[[Callable[..., Any]], Node])
assert_type(node(name="factory"), Callable[[Callable[..., Any]], Node])
