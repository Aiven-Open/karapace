"""
karapace - Test utils

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.utils import catch_and_raise_error

import pytest


def test_catch_and_raise_error():
    class RaiseMe(Exception):
        pass

    @catch_and_raise_error(to_catch=(ValueError,), to_raise=RaiseMe)
    def v():
        int("not a number")

    with pytest.raises(RaiseMe):
        v()

    @catch_and_raise_error(to_catch=(ZeroDivisionError,), to_raise=RaiseMe)
    def z():
        _ = 100 / 0

    with pytest.raises(RaiseMe):
        z()
