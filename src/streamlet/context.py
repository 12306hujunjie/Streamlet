"""依赖注入与验证工具。

BaseFlowContext + ContextVarProvider。
"""

import functools
import inspect
import logging
import weakref
from collections.abc import Callable, MutableMapping, MutableSequence, MutableSet
from contextvars import ContextVar
from typing import Any, Literal, cast, get_type_hints

from dependency_injector import containers, providers
from pydantic import (
    ConfigDict,
    PydanticUserError,
    TypeAdapter,
    ValidationError,
    validate_call,
)

from .exceptions import ValidationInputException, ValidationOutputException
from .retry import get_func_name

logger = logging.getLogger("streamlet")

# 模块级注册表——供 executor 做 fan-out 隔离
_CONTEXTVAR_PROVIDERS: weakref.WeakSet = weakref.WeakSet()
ContextCopyPolicy = Literal["shallow", "strict"]
_MUTABLE_VALUE_TYPES = (MutableMapping, MutableSequence, MutableSet, bytearray)
_IMMUTABLE_CONTAINER_TYPES = (tuple, frozenset)
_UNSET = object()


def _validate_copy_policy(copy_policy: str) -> ContextCopyPolicy:
    if copy_policy not in ("shallow", "strict"):
        raise ValueError(
            f"copy_policy must be 'shallow' or 'strict', got {copy_policy!r}"
        )
    return cast("ContextCopyPolicy", copy_policy)


def _find_nested_mutable_type(value: Any) -> type[Any] | None:
    if isinstance(value, _MUTABLE_VALUE_TYPES):
        return type(value)
    if isinstance(value, _IMMUTABLE_CONTAINER_TYPES):
        for item in value:
            mutable_type = _find_nested_mutable_type(item)
            if mutable_type is not None:
                return mutable_type
    return None


def _reject_nested_mutable_values(value: dict[Any, Any]) -> None:
    for key, item in value.items():
        mutable_type = _find_nested_mutable_type(item)
        if mutable_type is not None:
            raise ValueError(
                f"context key {key!r} contains nested mutable value "
                f"of type {mutable_type.__name__}; "
                "fan-out context isolation only shallow-copies the top-level dict"
            )


class ContextVarProvider(providers.Provider):
    """ContextVar 驱动的 Provider——同时支持线程安全和协程安全。"""

    def __init__(
        self,
        default_factory: Callable[[], Any] = dict,
        copy_policy: str = "shallow",
    ) -> None:
        super().__init__()
        self._context_var = ContextVar(f"streamlet_{id(self)}", default=_UNSET)
        self._default_factory = default_factory
        self._copy_policy = _validate_copy_policy(copy_policy)
        _CONTEXTVAR_PROVIDERS.add(self)

    def __deepcopy__(self, memo: dict[Any, Any] | None) -> "ContextVarProvider":
        # dependency-injector 会在容器实例化时 deepcopy providers；
        # 必须确保复制出来的新 provider 也被注册，且拥有独立的 ContextVar。
        if memo is None:
            memo = {}
        copy_obj = memo.get(id(self))
        if copy_obj is not None:
            return cast("ContextVarProvider", copy_obj)
        copy_obj = ContextVarProvider(
            self._default_factory,
            copy_policy=self._copy_policy,
        )
        memo[id(self)] = copy_obj
        return copy_obj

    def _provide(self, *args: Any, **kwargs: Any) -> Any:
        value = self._context_var.get()
        if value is _UNSET:
            value = self._default_factory()
            self._context_var.set(value)
        return value


def capture_context() -> dict[int, Any]:
    """捕获所有 ContextVarProvider 当前值的快照（fan-out 隔离用）。

    仅捕获已初始化的值，不触发 lazy init。
    """

    snapshot = {}
    for provider in list(_CONTEXTVAR_PROVIDERS):
        value = provider._context_var.get()
        if value is not _UNSET:
            snapshot[id(provider)] = value
    return snapshot


def apply_context(snapshot: dict[int, Any]) -> None:
    """在当前执行上下文中应用快照。dict 值做浅拷贝避免分支/线程间共享。

    同时用于：
    - AsyncExecutor.agather：asyncio Task 间隔离
    - SyncExecutor.gather：线程池 worker 间隔离（线程复用场景）
    """

    for provider in list(_CONTEXTVAR_PROVIDERS):
        val = snapshot.get(id(provider), _UNSET)
        if val is _UNSET:
            provider._context_var.set(_UNSET)
        elif isinstance(val, dict):
            if provider._copy_policy == "strict":
                _reject_nested_mutable_values(val)
            provider._context_var.set(dict(val))
        else:
            if provider._copy_policy == "strict":
                mutable_type = _find_nested_mutable_type(val)
                if mutable_type is not None:
                    raise ValueError(
                        f"context value contains mutable value of type "
                        f"{mutable_type.__name__}; fan-out context isolation cannot "
                        "copy non-dict mutable values safely"
                    )
            provider._context_var.set(val)


class BaseFlowContext(containers.DeclarativeContainer):
    """Flow context——线程安全 + 协程安全的 DI 容器。"""

    context: providers.Provider = ContextVarProvider(dict)


def _build_type_adapter(annotation: Any, config: ConfigDict) -> TypeAdapter[Any]:
    try:
        return TypeAdapter(annotation, config=config)
    except PydanticUserError as e:
        if e.code == "type-adapter-config-unused":
            return TypeAdapter(annotation)
        raise


def _create_input_validator_func(
    func: Callable[..., Any],
    sig: inspect.Signature,
) -> Callable[..., inspect.BoundArguments]:
    @functools.wraps(func)
    def validate_input_only(*args: Any, **kwargs: Any) -> inspect.BoundArguments:
        return sig.bind(*args, **kwargs)

    cast(Any, validate_input_only).__signature__ = sig
    return validate_input_only


def _custom_validate_call(
    validate_return: bool = True,
    config: ConfigDict | None = None,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """内部 validate_call 包装器，区分输入/输出验证异常。"""

    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)
        validation_config = config or ConfigDict(arbitrary_types_allowed=True)

        input_validator = validate_call(
            validate_return=False,
            config=validation_config,
        )(_create_input_validator_func(func, sig))

        return_type_adapter = None
        if validate_return and sig.return_annotation != inspect.Signature.empty:
            type_hints = get_type_hints(func, include_extras=True)
            return_annotation = type_hints.get("return", sig.return_annotation)
            return_type_adapter = _build_type_adapter(
                return_annotation,
                validation_config,
            )

        func_name = node_name or get_func_name(func)

        def create_input_exception(e: ValidationError) -> ValidationInputException:
            return ValidationInputException(
                f"输入参数验证失败: {e}",
                validation_error=e,
                node_name=func_name,
            )

        def create_output_exception(e: ValidationError) -> ValidationOutputException:
            return ValidationOutputException(
                f"返回值验证失败: {e}",
                validation_error=e,
                node_name=func_name,
            )

        def validate_result(result: Any) -> Any:
            if return_type_adapter:
                try:
                    return return_type_adapter.validate_python(result)
                except ValidationError as e:
                    raise create_output_exception(e) from e
            return result

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    bound_args = input_validator(*args, **kwargs)
                except ValidationError as e:
                    raise create_input_exception(e) from e
                result = await func(*bound_args.args, **bound_args.kwargs)
                return validate_result(result)

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    bound_args = input_validator(*args, **kwargs)
                except ValidationError as e:
                    raise create_input_exception(e) from e
                result = func(*bound_args.args, **bound_args.kwargs)
                return validate_result(result)

            return sync_wrapper

    return decorator
