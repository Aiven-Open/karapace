# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/ParsingTester.kt

from karapace.protobuf.location import Location
from karapace.protobuf.proto_parser import ProtoParser

import fnmatch
import os

# Recursively traverse a directory and attempt to parse all of its proto files.

#   Directory under which to search for protos. Change as needed.
src = "test"


def test_multi_files():
    total = 0
    failed = 0

    for root, dirnames, filenames in os.walk(src):  # pylint: disable=W0612
        for filename in fnmatch.filter(filenames, '*.proto'):
            fn = os.path.join(root, filename)
            print(f"Parsing {fn}")
            total += 1
            try:
                data = open(fn).read()
                ProtoParser.parse(Location.get(fn), data)
            except Exception as e:  # pylint: disable=broad-except
                print(e)
                failed += 1
    print(f"\nTotal: {total}  Failed: {failed}")
