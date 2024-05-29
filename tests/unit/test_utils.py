"""
karapace - Test utils

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.utils import intstr_conversion_guard

import pytest


def test_intstr_conversion_guard():
    class RaiseMe(Exception):
        pass

    @intstr_conversion_guard(to_raise=RaiseMe)
    def raise_value_error():
        int("not a number")

    with pytest.raises(RaiseMe):
        raise_value_error()
