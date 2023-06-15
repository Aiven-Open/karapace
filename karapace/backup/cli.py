"""
karapace - schema backup cli

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from . import api
from .errors import StaleConsumerError
from .poll_timeout import PollTimeout
from karapace.backup.api import VerifyLevel
from karapace.config import Config, read_config

import argparse
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Karapace schema backup tool")
    subparsers = parser.add_subparsers(help="Schema backup command", dest="command", required=True)

    parser_get = subparsers.add_parser("get", help="Store the schema backup into a file")
    parser_restore = subparsers.add_parser("restore", help="Restore the schema backup from a file")
    parser_inspect = subparsers.add_parser("inspect", help="Parse and dump metadata from a backup file.")
    parser_verify = subparsers.add_parser("verify", help="Parse metadata, and verify all checksums of a backup.")
    parser_export_anonymized_avro_schemas = subparsers.add_parser(
        "export-anonymized-avro-schemas", help="Export anonymized Avro schemas into a file"
    )

    for p in (parser_get, parser_restore, parser_inspect, parser_verify, parser_export_anonymized_avro_schemas):
        p.add_argument("--location", default="", help="File path for the backup file")

    for p in (parser_get, parser_restore, parser_export_anonymized_avro_schemas):
        p.add_argument("--config", help="Configuration file path", required=True)
        p.add_argument("--topic", help="Kafka topic name to be used", required=False)
        p.add_argument(
            "--skip-topic-creation",
            action="store_true",
            help="Skip topic creation, restoring only the data into an already existing topic.",
            default=False,
        )

    for p in (parser_get, parser_export_anonymized_avro_schemas):
        p.add_argument("--overwrite", action="store_true", help="Overwrite --location even if it exists.")
        p.add_argument("--poll-timeout", help=PollTimeout.__doc__, type=PollTimeout, default=PollTimeout.default())

    parser_get.add_argument("--use-format-v3", action="store_true", help="Use experimental V3 backup format.")
    parser_get.add_argument(
        "--replication-factor",
        help=(
            "Value will be stored in metadata, and used when creating topic during restoration. This is required for "
            "V3 backup, but has no effect on earlier versions, as they don't handle metadata."
        ),
        # This is hacky, but such is life with argparse.
        required="--use-format-v3" in sys.argv,
        type=int,
    )

    parser_verify.add_argument(
        "--level",
        choices=[level.value for level in VerifyLevel],
        required=True,
        help=(
            "At what level the backup should be verified. Use 'file' to only verify data file checksums. Use 'record' to "
            "also check that the files are fully parsable and that record counts and offsets are matching."
        ),
    )

    return parser.parse_args()


def get_config(args: argparse.Namespace) -> Config:
    with open(args.config) as buffer:
        return read_config(buffer)


def dispatch(args: argparse.Namespace) -> None:
    location = api.normalize_location(args.location)

    if args.command == "get":
        config = get_config(args)
        api.create_backup(
            config=config,
            backup_location=location,
            topic_name=api.normalize_topic_name(args.topic, config),
            version=api.BackupVersion.V3 if args.use_format_v3 else api.BackupVersion.V2,
            poll_timeout=args.poll_timeout,
            overwrite=args.overwrite,
            replication_factor=args.replication_factor,
        )
    elif args.command == "inspect":
        api.inspect(api.locate_backup_file(location))
    elif args.command == "verify":
        api.verify(api.locate_backup_file(location), level=VerifyLevel(args.level))
    elif args.command == "restore":
        config = get_config(args)
        api.restore_backup(
            config=config,
            backup_location=api.locate_backup_file(location),
            topic_name=api.normalize_topic_name(args.topic, config),
            skip_topic_creation=args.skip_topic_creation,
        )
    elif args.command == "export-anonymized-avro-schemas":
        config = get_config(args)
        api.create_backup(
            config=config,
            backup_location=location,
            topic_name=api.normalize_topic_name(args.topic, config),
            version=api.BackupVersion.ANONYMIZE_AVRO,
            poll_timeout=args.poll_timeout,
            overwrite=args.overwrite,
        )
    else:
        # Only reachable if a new subcommand was added that is not mapped above. There are other ways with
        # argparse to handle this, but all rely on the programmer doing exactly the right thing. Only switching
        # to another CLI framework would provide the ability to not handle this situation manually while
        # ensuring that it is not possible to add a new subcommand without also providing a handler for it.
        raise NotImplementedError(f"Unknown command: {args.command!r}")


def main() -> None:
    try:
        args = parse_args()

        try:
            dispatch(args)
        # TODO: This specific treatment of StaleConsumerError looks quite misplaced
        #  here, and should probably be pushed down into the (internal) API layer.
        except StaleConsumerError as e:
            print(
                f"The Kafka consumer did not receive any records for partition {e.topic_partition.partition} of topic "
                f"{e.topic_partition.topic!r} "
                f"within the poll timeout ({e.poll_timeout} seconds) while trying to reach offset {e.end_offset:,} "
                f"(start was {e.start_offset:,} and the last seen offset was {e.last_offset:,}).\n"
                "\n"
                "Try increasing --poll-timeout to give the broker more time.",
                file=sys.stderr,
            )
            raise SystemExit(1) from e
    except KeyboardInterrupt as e:
        # Not an error -- user choice -- and thus should not end up in a Python stacktrace.
        raise SystemExit(2) from e


if __name__ == "__main__":
    main()
