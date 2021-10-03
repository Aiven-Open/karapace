from karapace.protobuf.compare_restult import CompareResult
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.proto_parser import ProtoParser

location: Location = Location.get("some/folder", "file.proto")


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
    assert result.iscompatible()


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
    result = self_parsed.compare(other_parsed)
    assert result.iscompatible()


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
    result = self_parsed.compare(other_parsed)
    assert result.iscompatible()


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
    result = self_parsed.compare(other_parsed)
    assert result.iscompatible()


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
        |        string str = 1;
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
    result = self_parsed.compare(other_parsed)
    assert result.iscompatible()
