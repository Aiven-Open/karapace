"""
karapace

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from . import schema
from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType
from collections.abc import Generator
from karapace.avro_dataclasses.introspect import record_schema
from karapace.avro_dataclasses.models import AvroModel
from karapace.schema_models import parse_avro_schema_definition
from typing import Final

import argparse
import json
import pathlib
import shutil
import subprocess
import sys


def types() -> Generator[tuple[str, type[AvroModel]], None, None]:
    for name, value in schema.__dict__.items():
        try:
            if issubclass(value, AvroModel) and value != AvroModel:
                yield name, value
        except TypeError:
            continue


schema_directory: Final = pathlib.Path(__file__).parent.resolve() / "avro"
extension: Final = ".avsc"


def generate_schema() -> None:
    shutil.rmtree(schema_directory)
    schema_directory.mkdir()

    for name, schema_type in types():
        schema_file = schema_directory / f"{name}{extension}"
        print(f"Writing {schema_file.name} ...", end="")
        with schema_file.open("w") as fd:
            print(
                json.dumps(
                    record_schema(schema_type),
                    indent=2,
                    sort_keys=True,
                ),
                file=fd,
                flush=True,
            )
        print(" done.")


def relative_path(path: pathlib.Path) -> pathlib.Path:
    cwd = str(pathlib.Path.cwd())
    str_path = str(path)
    return pathlib.Path(str_path[len(cwd) + 1 :]) if str_path.startswith(cwd) else path


def target_has_source_layout(git_target: str) -> bool:
    cp = subprocess.run(["git", "show", f"{git_target}:src"], capture_output=True, check=False)
    if cp.returncode == 128:
        return False
    return True


def check_compatibility(git_target: str) -> None:
    errored = False
    found_any = False

    try:
        remote, branch = git_target.split("/", 1)
    except ValueError:
        remote = "origin"
        branch = git_target

    subprocess.run(["git", "fetch", remote, branch], check=True, capture_output=True)

    # Does the target version have source layout
    source_layout = target_has_source_layout(git_target)

    for file in schema_directory.glob(f"*{extension}"):
        relative = relative_path(file)
        if not source_layout:
            relative = pathlib.Path(*relative.parts[1:])
        with subprocess.Popen(
            ["git", "show", f"{git_target}:{relative}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as cp:
            std_out, std_err = cp.communicate()
        if cp.returncode == 128:
            print(
                f"⚠️  {file.name} does not exist on {git_target}, ignoring.",
                file=sys.stderr,
            )
            continue
        if cp.returncode != 0:
            raise RuntimeError(std_err)

        found_any = True
        target_schema = parse_avro_schema_definition(std_out.decode())
        new_schema = parse_avro_schema_definition(file.read_text())
        checker = ReaderWriterCompatibilityChecker()

        forwards_compat = checker.get_compatibility(
            reader=new_schema,
            writer=target_schema,
        )
        if forwards_compat.compatibility is not SchemaCompatibilityType.compatible:
            errored = True
            print(
                f"💥 Changes in {relative} breaks forwards compatibility with {git_target}.",
                file=sys.stderr,
            )

        backwards_compat = checker.get_compatibility(
            reader=target_schema,
            writer=new_schema,
        )
        if backwards_compat.compatibility is not SchemaCompatibilityType.compatible:
            errored = True
            print(
                f"💥 Changes in {relative} breaks backwards compatibility with {git_target}.",
                file=sys.stderr,
            )

    if errored:
        raise SystemExit(1)

    if not found_any:
        print(
            f"💥 Did not find any valid schemas on {git_target}.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print(f"✅ Schema is compatible with {git_target}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--against",
        default="main",
        help="git target to check compatibility against",
    )
    args = parser.parse_args()

    generate_schema()
    check_compatibility(args.against)


if __name__ == "__main__":
    main()
