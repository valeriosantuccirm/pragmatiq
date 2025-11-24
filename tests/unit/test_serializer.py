from typing import Any, Callable, Dict

import pytest

from pragmatiq.utils.serializers import deserialize_args, serialize_args


def test_serialize_args() -> None:
    def sample_func() -> None:
        pass

    # Successful cases
    assert serialize_args(args=(1, "hello", 3.14)) == (1, "hello", 3.14)
    assert serialize_args(args=(sample_func,)) == (
        {
            "type": "function",
            "name": "sample_func",
        },
    )
    assert serialize_args(args=(1, sample_func, "test")) == (
        1,
        {
            "type": "function",
            "name": "sample_func",
        },
        "test",
    )

    # Edge cases
    assert serialize_args(args=()) == ()
    assert serialize_args(args=(lambda x: x,)) == (
        {
            "type": "function",
            "name": "<lambda>",
        },
    )


def test_deserialize_args() -> None:
    def sample_func() -> None:
        pass

    func_mapping: Dict[str, Callable] = {"sample_func": sample_func}

    # Successful cases
    assert deserialize_args(args=(1, "hello", 3.14), func_mapping=func_mapping) == (1, "hello", 3.14)
    assert deserialize_args(
        args=(
            {
                "type": "function",
                "name": "sample_func",
            },
        ),
        func_mapping=func_mapping,
    ) == (sample_func,)
    assert deserialize_args(
        args=(
            1,
            {
                "type": "function",
                "name": "sample_func",
            },
            "test",
        ),
        func_mapping=func_mapping,
    ) == (1, sample_func, "test")

    # Failure cases
    with pytest.raises(expected_exception=ValueError, match="Function missing_func not found in mapping"):
        deserialize_args(
            args=(
                {
                    "type": "function",
                    "name": "missing_func",
                },
            ),
            func_mapping=func_mapping,
        )

    with pytest.raises(expected_exception=ValueError, match="Function name None not found in mapping"):
        deserialize_args(args=({"type": "function"},), func_mapping=func_mapping)

    with pytest.raises(expected_exception=ValueError, match="Function name None not found in mapping"):
        deserialize_args(
            args=(
                {
                    "type": "function",
                    "name": None,
                },
            ),
            func_mapping=func_mapping,
        )


def test_edge_cases() -> None:
    func_mapping: Dict[Any, Any] = {}

    # Empty input should return empty output
    assert deserialize_args(args=(), func_mapping=func_mapping) == ()

    # Function with an empty mapping should raise an error
    with pytest.raises(expected_exception=ValueError, match="Function sample_func not found in mapping"):
        deserialize_args(
            args=(
                {
                    "type": "function",
                    "name": "sample_func",
                },
            ),
            func_mapping=func_mapping,
        )
