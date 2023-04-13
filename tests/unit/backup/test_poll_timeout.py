"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from datetime import timedelta
from karapace.backup.poll_timeout import PollTimeout
from typing import Union

import pytest


class TestPollTimeout:
    @pytest.mark.parametrize("it", ("PT0.999S", timedelta(milliseconds=999)))
    def test_min_validation(self, it: Union[str, timedelta]) -> None:
        with pytest.raises(ValueError) as e:
            PollTimeout(it)
        assert str(e.value) == "Poll timeout MUST be at least one second, got: PT0.999S"

    # Changing the default is not a breaking change, but the documentation needs to be adjusted!
    def test_default(self) -> None:
        assert str(PollTimeout.default()) == "PT1M"

    def test__str__(self) -> None:
        assert str(PollTimeout.of(seconds=1, milliseconds=500)) == "PT1.5S"

    def test__repr__(self) -> None:
        assert repr(PollTimeout.of(seconds=1, milliseconds=500)) == "PollTimeout(value='PT1.5S')"

    def test_milliseconds(self) -> None:
        assert PollTimeout(timedelta(milliseconds=1000.5)).milliseconds == 1000
