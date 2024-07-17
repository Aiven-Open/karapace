"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.compare_result import CompareResult, Modification
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.proto_parser import ProtoParser
from karapace.protobuf.protopace import check_compatibility, Proto

import pytest

location: Location = Location("some/folder", "file.proto")


def test_compatibility_package():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a2;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a2.TestMessage.Value val = 2;
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)
    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)
    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()


def test_compatibility_package_with_protopace():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a2;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a2.TestMessage.Value val = 2;
        |}
        |"""
    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)

    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    check_compatibility(proto, prev_proto)


def test_compatibility_field_add():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |        string str2 = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)
    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)
    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()


def test_compatibility_field_add_with_protopace():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |        string str2 = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)
    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)
    check_compatibility(proto, prev_proto)


def test_compatibility_field_drop():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |        string str2 = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)
    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)
    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()


def test_compatibility_field_drop_with_protopace():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |        string str2 = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)

    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    check_compatibility(proto, prev_proto)


def test_compatibility_field_add_drop():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str2 = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)
    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)
    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()


def test_compatibility_field_add_drop_with_protopace():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str2 = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str = 1;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)

    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    check_compatibility(proto, prev_proto)


def test_compatibility_enum_add():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str2 = 1;
        |        int32 x = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str2 = 1;
        |        Enu x = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |    enum Enu {
        |        A = 0;
        |        B = 1;
        |    }
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)
    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)

    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()


def test_compatibility_enum_add_with_protopace():
    self_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str2 = 1;
        |        int32 x = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |}
        |"""

    other_schema = """
        |syntax = "proto3";
        |package a1;
        |message TestMessage {
        |    message Value {
        |        string str2 = 1;
        |        Enu x = 2;
        |    }
        |    string test = 1;
        |    .a1.TestMessage.Value val = 2;
        |    enum Enu {
        |        A = 0;
        |        B = 1;
        |    }
        |}
        |"""

    self_schema = trim_margin(self_schema)
    other_schema = trim_margin(other_schema)

    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    # Note: This will be interpreted as a type change and will be regognized as breaking with vanilla buf
    with pytest.raises(Exception) as e:
        check_compatibility(proto, prev_proto)
    assert 'Field "2" with name "x" on message "Value" changed type from "int32" to "enum".' in str(e)


def test_compatibility_ordering_change_msg():
    self_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  int32 fredfield = 1;
}

message HodoCode {
  int32 hodofield = 0;
}
"""

    other_schema = """\
syntax = "proto3";
package tc4;

message HodoCode {
  int32 hodofield = 0;
}

message Fred {
  int32 fredfield = 1;
}
"""

    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)

    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()
    assert len(result.result) == 0


def test_compatibility_ordering_change_msg_with_protopace():
    self_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  int32 fredfield = 1;
}

message HodoCode {
  int32 hodofield = 1;
}
"""

    other_schema = """\
syntax = "proto3";
package tc4;

message HodoCode {
  int32 hodofield = 1;
}

message Fred {
  int32 fredfield = 1;
}
"""
    # Note: tag number must be > 0. Had to be changed from original test.
    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    check_compatibility(proto, prev_proto)


def test_compatibility_ordering_change():
    self_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  HodoCode hodecode = 1;
}

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}
"""

    other_schema = """\
syntax = "proto3";
package tc4;

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}

message Fred {
  HodoCode hodecode = 1;
  string id = 2;
}
"""

    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)

    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()
    assert len(result.result) == 1
    assert result.result[0].modification == Modification.FIELD_ADD


def test_compatibility_ordering_change_with_protopace():
    self_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  HodoCode hodecode = 1;
}

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}
"""

    other_schema = """\
syntax = "proto3";
package tc4;

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}

message Fred {
  HodoCode hodecode = 1;
  string id = 2;
}
"""

    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    check_compatibility(proto, prev_proto)


def test_compatibility_ordering_change2():
    self_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  HodoCode hodecode = 1;
}

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}
"""

    other_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  HodoCode hodecode = 1;
  string id = 2;
}

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}
"""

    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)

    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert result.is_compatible()
    assert len(result.result) == 1
    assert result.result[0].modification == Modification.FIELD_ADD


def test_compatibility_ordering_change2_with_protopace():
    self_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  HodoCode hodecode = 1;
}

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}
"""

    other_schema = """\
syntax = "proto3";
package tc4;

message Fred {
  HodoCode hodecode = 1;
  string id = 2;
}

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}
"""

    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    check_compatibility(proto, prev_proto)


def test_compatibility_field_tag_change():
    self_schema = """\
syntax = "proto3";
package pkg;
message Foo {
    string fieldA = 1;
    string fieldB = 2;
    string fieldC = 3;
    string fieldX = 4;
}
"""

    other_schema = """\
syntax = "proto3";
package pkg;
message Foo {
    string fieldA = 1;
    string fieldB = 2;
    string fieldC = 3;
    string fieldX = 5;
}
"""

    self_parsed: ProtoFileElement = ProtoParser.parse(location, self_schema)
    other_parsed: ProtoFileElement = ProtoParser.parse(location, other_schema)

    result = CompareResult()
    self_parsed.compare(other_parsed, result)
    assert not result.is_compatible()
    assert {(error.modification, error.path) for error in result.result} == {
        (Modification.FIELD_TAG_ALTER, "Foo.fieldX"),
        (Modification.FIELD_DROP, "Foo.4"),
        (Modification.FIELD_ADD, "Foo.5"),
    }


def test_compatibility_field_tag_change_with_protopace():
    self_schema = """\
syntax = "proto3";
package pkg;
message Foo {
    string fieldA = 1;
    string fieldB = 2;
    string fieldC = 3;
    string fieldX = 4;
}
"""

    other_schema = """\
syntax = "proto3";
package pkg;
message Foo {
    string fieldA = 1;
    string fieldB = 2;
    string fieldC = 3;
    string fieldX = 5;
}
"""

    proto = Proto("test.proto", other_schema)
    prev_proto = Proto("test.proto", self_schema)

    # Note: because we allow field name change this is not recognized as incompatible
    check_compatibility(proto, prev_proto)
