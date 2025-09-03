"""Tests for custom_validate_call validation wrapper."""

import pytest
from pydantic import BaseModel

from src.aetherflow import (
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

    def test_pydantic_output_validation_fails(self):
        UserModel = self.UserModel

        @custom_validate_call()
        def create_user(name: str, age: int) -> UserModel:
            return 42  # int cannot be coerced to UserModel

        with pytest.raises(ValidationOutputException):
            create_user("Bob", 25)


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
