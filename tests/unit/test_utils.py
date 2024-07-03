"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.utils import remove_prefix


def test_remove_prefix_basic() -> None:
    result = remove_prefix("hello world", "hello ")
    assert result == "world"


def test_remove_prefix_empty_prefix() -> None:
    result = remove_prefix("hello world", "")
    assert result == "hello world"


def test_remove_prefix_prefix_not_in_string() -> None:
    result = remove_prefix("hello world", "hey ")
    assert result == "hello world"


def test_remove_prefix_multiple_occurrences_of_prefix() -> None:
    result = remove_prefix("hello hello world", "hello ")
    assert result == "hello world"


def test_remove_prefix_empty_string() -> None:
    result = remove_prefix("", "hello ")
    assert result == ""
