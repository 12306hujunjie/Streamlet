"""Tests for custom_validate_call validation wrapper."""

import importlib.util
import sys

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from streamlet import (
    ValidationInputException,
    ValidationOutputException,
    custom_validate_call,
)


class TestInputValidation:
    def test_valid_input_passes(self):
        @custom_validate_call()
        def double(x: int) -> int:
            return x * 2

        result = double(5)
        assert result == 10

    def test_invalid_input_type_raises(self):
        @custom_validate_call()
        def double(x: int) -> int:
            return x * 2

        with pytest.raises(ValidationInputException):
            double("not_an_int")

    def test_input_validation_error_contains_node_name(self):
        @custom_validate_call(node_name="test_node")
        def double(x: int) -> int:
            return x * 2

        with pytest.raises(ValidationInputException) as exc_info:
            double("not_an_int")
        assert exc_info.value.node_name == "test_node"

    def test_validation_input_exception_attributes(self):
        @custom_validate_call(node_name="my_node")
        def process(x: int) -> int:
            return x

        with pytest.raises(ValidationInputException) as exc_info:
            process("bad")
        assert exc_info.value.retryable is False
        assert exc_info.value.validation_error is not None


class TestOutputValidation:
    def test_valid_output_passes(self):
        @custom_validate_call()
        def double(x: int) -> int:
            return x * 2

        result = double(5)
        assert result == 10

    def test_invalid_output_type_raises(self):
        @custom_validate_call()
        def bad_return(x: int) -> str:
            return 42  # returns int, annotated as str

        with pytest.raises(ValidationOutputException):
            bad_return(5)

    def test_output_validation_error_contains_original(self):
        @custom_validate_call(node_name="output_node")
        def bad_return(x: int) -> str:
            return 42

        with pytest.raises(ValidationOutputException) as exc_info:
            bad_return(5)
        assert exc_info.value.node_name == "output_node"
        assert exc_info.value.validation_error is not None


class TestPydanticModelValidation:
    class UserModel(BaseModel):
        name: str
        age: int

    def test_valid_pydantic_input(self):
        UserModel = self.UserModel

        @custom_validate_call()
        def process_user(user: UserModel) -> dict:
            return {"name": user.name, "age": user.age}

        result = process_user(UserModel(name="Alice", age=30))
        assert result["name"] == "Alice"

    def test_dict_input_fails_validation(self):
        UserModel = self.UserModel

        @custom_validate_call()
        def process_user(user: UserModel) -> dict:
            return {"name": user.name, "age": user.age}

        with pytest.raises(ValidationInputException):
            process_user({"name": "Alice"})

    def test_pydantic_output_validation(self):
        UserModel = self.UserModel

        @custom_validate_call()
        def create_user(name: str, age: int) -> UserModel:
            return UserModel(name=name, age=age)

        result = create_user("Bob", 25)
        assert result.name == "Bob"

    def test_pydantic_output_returns_validated_value(self):
        UserModel = self.UserModel

        @custom_validate_call()
        def create_user(name: str, age: str) -> UserModel:
            return {"name": name, "age": age}

        result = create_user("Bob", "25")
        assert isinstance(result, UserModel)
        assert result.age == 25

    def test_pydantic_output_uses_model_config_when_call_config_is_passed(self):
        class LowercaseUserModel(BaseModel):
            model_config = ConfigDict(str_to_lower=True)

            name: str

        @custom_validate_call(config=ConfigDict(str_to_upper=True))
        def create_user(name: str) -> LowercaseUserModel:
            return {"name": name}

        result = create_user("ALICE")
        assert result.name == "alice"

    def test_pydantic_output_validation_fails(self):
        UserModel = self.UserModel

        @custom_validate_call()
        def create_user(name: str, age: int) -> UserModel:
            return 42  # int cannot be coerced to UserModel

        with pytest.raises(ValidationOutputException):
            create_user("Bob", 25)

    def test_function_body_validation_error_is_not_wrapped_as_input_error(self):
        class Payload(BaseModel):
            x: int

        @custom_validate_call()
        def create_payload() -> None:
            Payload(x="bad")

        with pytest.raises(ValidationError):
            create_payload()

    def test_postponed_return_annotation_validates_output_model(self, tmp_path):
        module_path = tmp_path / "future_annotations_module.py"
        module_path.write_text(
            """
from __future__ import annotations

from pydantic import BaseModel

from streamlet import custom_validate_call


class User(BaseModel):
    name: str
    age: int


@custom_validate_call()
def create_user() -> User:
    return {"name": "Alice", "age": "30"}


@custom_validate_call()
def create_invalid_user() -> User:
    return 42
""",
            encoding="utf-8",
        )
        module_name = "future_annotations_module"
        spec = importlib.util.spec_from_file_location(
            module_name,
            module_path,
        )
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            sys.modules.pop(module_name, None)

        result = module.create_user()

        assert isinstance(result, module.User)
        assert result.age == 30
        with pytest.raises(ValidationOutputException):
            module.create_invalid_user()


class TestAsyncValidation:
    @pytest.mark.asyncio
    async def test_async_valid_input_passes(self):
        @custom_validate_call()
        async def double(x: int) -> int:
            return x * 2

        result = await double(5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_async_invalid_input_raises(self):
        @custom_validate_call()
        async def double(x: int) -> int:
            return x * 2

        with pytest.raises(ValidationInputException):
            await double("bad")

    @pytest.mark.asyncio
    async def test_async_invalid_output_raises(self):
        @custom_validate_call()
        async def bad_return(x: int) -> str:
            return 42

        with pytest.raises(ValidationOutputException):
            await bad_return(5)

    @pytest.mark.asyncio
    async def test_async_function_body_validation_error_is_not_wrapped(self):
        class Payload(BaseModel):
            x: int

        @custom_validate_call()
        async def create_payload() -> None:
            Payload(x="bad")

        with pytest.raises(ValidationError):
            await create_payload()
