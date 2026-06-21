"""公共协议类型。"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class RepeatInputMode(Enum):
    """Repeat 输入传递策略。"""

    PREVIOUS_RESULT = "previous_result"
    SAME_INPUT = "same_input"


@dataclass(frozen=True)
class CallArgs:
    """显式下一轮调用参数协议。"""

    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", dict(self.kwargs))


def call_args(*args: Any, **kwargs: Any) -> CallArgs:
    """Create explicit args/kwargs for the next repeat iteration."""
    return CallArgs(args=args, kwargs=kwargs)
