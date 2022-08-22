"""
karapace - Test base class
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from dataclasses import dataclass


@dataclass
class BaseTestCase:
    test_name: str

    def __str__(self):
        return self.test_name
