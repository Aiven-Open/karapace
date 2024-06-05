"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
import aiokafka.codec


# Test that the setup has all compression algorithms supported
def test_setup_features() -> None:
    assert aiokafka.codec.has_gzip()
    assert aiokafka.codec.has_lz4()
    assert aiokafka.codec.has_snappy()
    assert aiokafka.codec.has_zstd()
