"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""


class DecodeRecordError(Exception):
    pass


class InvalidChecksum(DecodeRecordError, ValueError):
    pass


class InvalidBytesConsumed(DecodeRecordError, ValueError):
    pass
