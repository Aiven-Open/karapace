"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from datetime import timedelta
from karapace.backup.poll_timeout import PollTimeout

import pytest


class TestPollTimeout:
    @pytest.mark.parametrize("it", ("PT0.999S", timedelta(milliseconds=999)))
    def test_min_validation(self, it: str | timedelta) -> None:
        with pytest.raises(
            ValueError,
            match=r"^Poll timeout must be at least one second, got: 0:00:00.999000$",
        ):
            PollTimeout(it)

    def test_max_validation(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"^Poll timeout must be less than one year, got: 1 years, 0:00:00$",
        ):
            PollTimeout("P1Y")

    # Changing the default is not a breaking change, but the documentation needs to be adjusted!
    def test_default(self) -> None:
        assert str(PollTimeout.default()) == "PT1M"

    def test__str__(self) -> None:
        assert str(PollTimeout.of(seconds=1, milliseconds=500)) == "PT1.5S"

    def test__repr__(self) -> None:
        assert repr(PollTimeout.of(seconds=1, milliseconds=500)) == "PollTimeout(value='PT1.5S')"

    def test_milliseconds(self) -> None:
        assert PollTimeout(timedelta(milliseconds=1000.5)).milliseconds == 1000

    def test_seconds(self) -> None:
        assert PollTimeout(timedelta(milliseconds=1500)).seconds == 1.5
