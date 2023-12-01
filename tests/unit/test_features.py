"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
import kafka.codec


# Test that the setup has all compression algorithms supported
def test_setup_features() -> None:
    assert kafka.codec.has_gzip()
    assert kafka.codec.has_lz4()
    assert kafka.codec.has_snappy()
    assert kafka.codec.has_zstd()
