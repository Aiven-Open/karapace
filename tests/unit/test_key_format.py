"""
karapace - Test key format

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from dataclasses import dataclass
from karapace.key_format import is_key_in_canonical_format, KeyFormatter, KeyMode
from karapace.typing import JsonData
from tests.base_testcase import BaseTestCase

import pytest


@dataclass
class KeyFormatCase(BaseTestCase):
    keymode: KeyMode
    input: JsonData
    expected: JsonData


@dataclass
class IsInCanonicalFormatCase(BaseTestCase):
    input: JsonData
    expected: bool


def test_keymode_set_and_get() -> None:
    key_formatter = KeyFormatter()
    assert key_formatter.get_keymode() == KeyMode.CANONICAL
    key_formatter.set_keymode(KeyMode.DEPRECATED_KARAPACE)
    assert key_formatter.get_keymode() == KeyMode.DEPRECATED_KARAPACE
    key_formatter.set_keymode(KeyMode.CANONICAL)
    assert key_formatter.get_keymode() == KeyMode.CANONICAL


@pytest.mark.parametrize(
    "testcase",
    [
        # Canonical format
        KeyFormatCase(
            test_name="Schema message, Karapace format to canonical",
            keymode=KeyMode.CANONICAL,
            input={
                "subject": "test",
                "version": 1,
                "magic": 1,
                "keytype": "SCHEMA",
            },
            expected=b'{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}',
        ),
        KeyFormatCase(
            test_name="Config message, has subject, Karapace format to canonical",
            keymode=KeyMode.CANONICAL,
            input={
                "subject": "test",
                "magic": 0,
                "keytype": "CONFIG",
            },
            expected=b'{"keytype":"CONFIG","subject":"test","magic":0}',
        ),
        KeyFormatCase(
            test_name="Config message, None subject, Karapace format to canonical",
            keymode=KeyMode.CANONICAL,
            input={
                "subject": None,
                "magic": 0,
                "keytype": "CONFIG",
            },
            expected=b'{"keytype":"CONFIG","subject":null,"magic":0}',
        ),
        KeyFormatCase(
            test_name="Delete subject message, Karapace format to canonical",
            keymode=KeyMode.CANONICAL,
            input={
                "subject": "test",
                "magic": 0,
                "keytype": "DELETE_SUBJECT",
            },
            expected=b'{"keytype":"DELETE_SUBJECT","subject":"test","magic":0}',
        ),
        KeyFormatCase(
            test_name="NOOP message, Karapace format to canonical",
            keymode=KeyMode.CANONICAL,
            input={
                "magic": 0,
                "keytype": "NOOP",
            },
            expected=b'{"keytype":"NOOP","magic":0}',
        ),
        # Karapace format
        KeyFormatCase(
            test_name="Schema message, Karapace format",
            keymode=KeyMode.DEPRECATED_KARAPACE,
            input={
                "subject": "test",
                "version": 1,
                "magic": 1,
                "keytype": "SCHEMA",
            },
            expected=b'{"subject":"test","version":1,"magic":1,"keytype":"SCHEMA"}',
        ),
        KeyFormatCase(
            test_name="Config message, has subject, Karapace format",
            keymode=KeyMode.DEPRECATED_KARAPACE,
            input={
                "subject": "test",
                "magic": 0,
                "keytype": "CONFIG",
            },
            expected=b'{"subject":"test","magic":0,"keytype":"CONFIG"}',
        ),
        KeyFormatCase(
            test_name="Config message, None subject, Karapace format",
            keymode=KeyMode.DEPRECATED_KARAPACE,
            input={
                "subject": None,
                "magic": 0,
                "keytype": "CONFIG",
            },
            expected=b'{"subject":null,"magic":0,"keytype":"CONFIG"}',
        ),
        KeyFormatCase(
            test_name="Delete subject message, Karapace format",
            keymode=KeyMode.DEPRECATED_KARAPACE,
            input={
                "subject": "test",
                "magic": 0,
                "keytype": "DELETE_SUBJECT",
            },
            expected=b'{"subject":"test","magic":0,"keytype":"DELETE_SUBJECT"}',
        ),
        KeyFormatCase(
            test_name="NOOP message, Karapace format",
            keymode=KeyMode.DEPRECATED_KARAPACE,
            input={
                "magic": 0,
                "keytype": "NOOP",
            },
            expected=b'{"magic":0,"keytype":"NOOP"}',
        ),
    ],
)
def test_format_key(testcase: KeyFormatCase) -> None:
    assert KeyFormatter().format_key(key=testcase.input, keymode=testcase.keymode) == testcase.expected


@pytest.mark.parametrize(
    "testcase",
    [
        # Canonical format
        IsInCanonicalFormatCase(
            test_name="Schema message, canonical format",
            input={
                "keytype": "SCHEMA",
                "subject": "test",
                "version": 1,
                "magic": 1,
            },
            expected=True,
        ),
        IsInCanonicalFormatCase(
            test_name="Config message, canonical format",
            input={
                "keytype": "CONFIG",
                "subject": "test",
                "magic": 0,
            },
            expected=True,
        ),
        IsInCanonicalFormatCase(
            test_name="Config message, canonical format",
            input={
                "keytype": "CONFIG",
                "subject": None,
                "magic": 0,
            },
            expected=True,
        ),
        IsInCanonicalFormatCase(
            test_name="Delete subject message, canonical format",
            input={
                "keytype": "DELETE_SUBJECT",
                "subject": "test",
                "magic": 0,
            },
            expected=True,
        ),
        IsInCanonicalFormatCase(
            test_name="NOOP message, canonical format",
            input={
                "keytype": "NOOP",
                "magic": 0,
            },
            expected=True,
        ),
        # Karapace format
        IsInCanonicalFormatCase(
            test_name="Schema message, Karapace format",
            input={
                "subject": "test",
                "version": 1,
                "magic": 1,
                "keytype": "SCHEMA",
            },
            expected=False,
        ),
        IsInCanonicalFormatCase(
            test_name="Config message, has subject, Karapace format",
            input={
                "subject": "test",
                "magic": 0,
                "keytype": "CONFIG",
            },
            expected=False,
        ),
        IsInCanonicalFormatCase(
            test_name="Config message, None subject, Karapace format",
            input={
                "subject": None,
                "magic": 0,
                "keytype": "CONFIG",
            },
            expected=False,
        ),
        IsInCanonicalFormatCase(
            test_name="Delete subject message, Karapace format",
            input={
                "subject": "test",
                "magic": 0,
                "keytype": "DELETE_SUBJECT",
            },
            expected=False,
        ),
        IsInCanonicalFormatCase(
            test_name="NOOP message, Karapace format",
            input={
                "magic": 0,
                "keytype": "NOOP",
            },
            expected=False,
        ),
    ],
)
def test_is_in_canonical_format(testcase: IsInCanonicalFormatCase) -> None:
    assert is_key_in_canonical_format(testcase.input) == testcase.expected
