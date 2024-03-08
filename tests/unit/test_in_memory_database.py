"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.in_memory_database import InMemoryDatabase, Subject


class TestFindSchemas:
    def test_returns_empty_list_when_no_schemas(self) -> None:
        database = InMemoryDatabase()
        subject = Subject("hello_world")
        database.insert_subject(subject=subject)
        expected = {subject: []}
        assert database.find_schemas(include_deleted=True, latest_only=True) == expected
