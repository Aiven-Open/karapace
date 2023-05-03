"""
karapace - schema backup cli

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .api import BackupVersion, SchemaBackup
from .consumer import PollTimeout
from .errors import StaleConsumerError
from karapace.config import read_config

import argparse
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Karapace schema backup tool")
    subparsers = parser.add_subparsers(help="Schema backup command", dest="command", required=True)

    parser_get = subparsers.add_parser("get", help="Store the schema backup into a file")
    parser_restore = subparsers.add_parser("restore", help="Restore the schema backup from a file")
    parser_export_anonymized_avro_schemas = subparsers.add_parser(
        "export-anonymized-avro-schemas", help="Export anonymized Avro schemas into a file"
    )
    for p in (parser_get, parser_restore, parser_export_anonymized_avro_schemas):
        p.add_argument("--config", help="Configuration file path", required=True)
        p.add_argument("--location", default="", help="File path for the backup file")
        p.add_argument("--topic", help="Kafka topic name to be used", required=False)

    for p in (parser_get, parser_export_anonymized_avro_schemas):
        p.add_argument("--overwrite", action="store_true", help="Overwrite --location even if it exists.")
        p.add_argument("--poll-timeout", help=PollTimeout.__doc__, type=PollTimeout)

    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()

        with open(args.config, encoding="utf8") as handler:
            config = read_config(handler)

        sb = SchemaBackup(config, args.location, args.topic)

        try:
            if args.command == "get":
                sb.create(
                    BackupVersion.V2,
                    poll_timeout=args.poll_timeout,
                    overwrite=args.overwrite,
                )
            elif args.command == "restore":
                sb.restore_backup()
            elif args.command == "export-anonymized-avro-schemas":
                sb.create(
                    BackupVersion.ANONYMIZE_AVRO,
                    poll_timeout=args.poll_timeout,
                    overwrite=args.overwrite,
                )
            else:
                # Only reachable if a new subcommand was added that is not mapped above. There are other ways with
                # argparse to handle this, but all rely on the programmer doing exactly the right thing. Only switching
                # to another CLI framework would provide the ability to not handle this situation manually while
                # ensuring that it is not possible to add a new subcommand without also providing a handler for it.
                raise SystemExit(f"Entered unreachable code, unknown command: {args.command!r}")
        except StaleConsumerError as e:
            print(
                f"The Kafka consumer did not receive any records for partition {e.partition} of topic {e.topic!r} "
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
