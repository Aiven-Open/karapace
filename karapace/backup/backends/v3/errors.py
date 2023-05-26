"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.backup.errors import BackupError


class DecodeError(BackupError):
    pass


class InvalidChecksum(DecodeError, ValueError):
    pass


class InvalidHeader(DecodeError, ValueError):
    pass


class RecordCountMismatch(DecodeError, ValueError):
    pass


class TooManyRecords(RecordCountMismatch):
    pass


class TooFewRecords(RecordCountMismatch):
    pass


class UnknownChecksumAlgorithm(DecodeError, NotImplementedError):
    pass


class UnexpectedEndOfData(DecodeError, ValueError):
    pass


class EncodeError(BackupError):
    pass


class IntegerBelowBound(EncodeError, ValueError):
    pass


class IntegerAboveBound(EncodeError, ValueError):
    pass
