"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from functools import partial
from karapace.core.backup.backends.reader import ProducerSend, RestoreTopicLegacy
from karapace.core.backup.backends.v1 import SchemaBackupV1Reader
from karapace.core.backup.encoders import encode_key, encode_value
from karapace.core.key_format import KeyFormatter
from pathlib import Path

import json
import textwrap


def get_reader() -> SchemaBackupV1Reader:
    return SchemaBackupV1Reader(
        key_encoder=partial(encode_key, key_formatter=KeyFormatter()),
        value_encoder=encode_value,
    )


class TestSchemaBackupV1Reader:
    def test_yields_instructions_from_json_array(self, tmp_file: Path) -> None:
        reader = get_reader()
        tmp_file.write_text(
            textwrap.dedent(
                """\
                [
                    [{"subject": "subject-1", "keytype": "SCHEMA", "magic": 1}, {"foo": "bar"}],
                    [{"subject": "subject-2", "keytype": "SCHEMA", "magic": 1}, {"foo": null}]
                ]
                """
            )
        )

        (
            restore_topic,
            first_send,
            second_send,
        ) = tuple(reader.read(tmp_file, "some-topic"))

        assert restore_topic == RestoreTopicLegacy(topic_name="some-topic", partition_count=1)

        # First message.
        assert isinstance(first_send, ProducerSend)
        assert first_send.topic_name == "some-topic"
        assert first_send.partition_index == 0
        assert json.loads(first_send.key) == {
            "subject": "subject-1",
            "keytype": "SCHEMA",
            "magic": 1,
        }
        assert json.loads(first_send.value) == {"foo": "bar"}

        # Second message.
        assert isinstance(second_send, ProducerSend)
        assert second_send.topic_name == "some-topic"
        assert second_send.partition_index == 0
        assert json.loads(second_send.key) == {
            "subject": "subject-2",
            "keytype": "SCHEMA",
            "magic": 1,
        }
        assert json.loads(second_send.value) == {"foo": None}

    def test_yields_single_restore_topic_for_null(self, tmp_file: Path) -> None:
        reader = get_reader()
        tmp_file.write_text("null")

        assert tuple(reader.read(tmp_file, "some-topic")) == (
            RestoreTopicLegacy(topic_name="some-topic", partition_count=1),
        )
