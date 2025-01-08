"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

schema_protobuf_plain = """\
syntax = "proto3";
package com.codingharbour.protobuf;

option java_outer_classname = "SimpleMessageProtos";

message SimpleMessage {
  string content = 1;
  string date_time = 2;
  string content2 = 3;
}
"""

schema_protobuf_plain_bin = (
    "CgdkZWZhdWx0Ehpjb20uY29kaW5naGFyYm91ci5wcm90b2J1ZiI/Cg1TaW1wbGVNZXNzYW"
    + "dlEg0KB2NvbnRlbnQYASgJEg8KCWRhdGVfdGltZRgCKAkSDgoIY29udGVudDIYAygJQh"
    + "VCE1NpbXBsZU1lc3NhZ2VQcm90b3NiBnByb3RvMw=="
)

# Created using protoc
schema_protobuf_plain_bin_protoc = (
    "Chl0ZXN0cy9zY2hlbWFzL3BsYWluLnByb3RvEhpjb20uY29kaW5naGFyYm91ci5wcm90b2"
    "J1ZiJFCg1TaW1wbGVNZXNzYWdlEg8KB2NvbnRlbnQYASABKAkSEQoJZGF0ZV90aW1lGAIg"
    "ASgJEhAKCGNvbnRlbnQyGAMgASgJQhVCE1NpbXBsZU1lc3NhZ2VQcm90b3NiBnByb3RvMw=="
)

schema_protobuf_schema_registry1 = """\
syntax = "proto3";
package com.codingharbour.protobuf;

message SimpleMessage {
  string content = 1;
  string my_string = 2;
  int32 my_int = 3;
}

"""

schema_protobuf_order_before = """\
syntax = "proto3";

option java_package = "com.codingharbour.protobuf";
option java_outer_classname = "TestEnumOrder";

enum Enum {
  HIGH = 0;
  MIDDLE = 1;
  LOW = 2;
}
message Message {
  int32 query = 1;
}
"""

schema_protobuf_order_after = """\
syntax = "proto3";

option java_package = "com.codingharbour.protobuf";
option java_outer_classname = "TestEnumOrder";

message Message {
  int32 query = 1;
}
enum Enum {
  HIGH = 0;
  MIDDLE = 1;
  LOW = 2;
}

"""

schema_protobuf_order_after_bin = (
    "CgdkZWZhdWx0IhYKB01lc3NhZ2USCwoFcXVlcnkYASgFKiUKBEVudW0SCAoESElHSBAAEg"
    + "oKBk1JRERMRRABEgcKA0xPVxACQisKGmNvbS5jb2RpbmdoYXJib3VyLnByb3RvYnVmQg"
    "1UZXN0RW51bU9yZGVyYgZwcm90bzM="
)


schema_protobuf_compare_one = """
|syntax = "proto3";
|
|option java_package = "com.codingharbour.protobuf";
|option java_outer_classname = "TestEnumOrder";
|
|message Message {
|  int32 query = 1;
|  string content = 2;
|}
|enum Enum {
|  HIGH = 0;
|  MIDDLE = 1;
|  LOW = 2;
|}
|
"""

schema_protobuf_nested_message4 = """\
syntax = "proto3";
package fancy.company.in.party.v1;

message AnotherMessage {
  message WowANestedMessage {
    message DeeplyNestedMsg {
      message AnotherLevelOfNesting {
        .fancy.company.in.party.v1.AnotherMessage.WowANestedMessage.BamFancyEnum im_tricky_im_referring_to_the_previous_enum = 1;
      }
    }
    enum BamFancyEnum {
      MY_AWESOME_FIELD = 0;
    }
  }
}
"""

schema_protobuf_nested_message4_bin = (
    "CgdkZWZhdWx0EhlmYW5jeS5jb21wYW55LmluLnBhcnR5LnYxIvUBCg5Bbm90aGVyTWVzc2"
    + "FnZRriAQoRV293QU5lc3RlZE1lc3NhZ2UapgEKD0RlZXBseU5lc3RlZE1zZxqSAQoVQW"
    + "5vdGhlckxldmVsT2ZOZXN0aW5nEnkKK2ltX3RyaWNreV9pbV9yZWZlcnJpbmdfdG9fdG"
    + "hlX3ByZXZpb3VzX2VudW0YATJILmZhbmN5LmNvbXBhbnkuaW4ucGFydHkudjEuQW5vdG"
    + "hlck1lc3NhZ2UuV293QU5lc3RlZE1lc3NhZ2UuQmFtRmFuY3lFbnVtIiQKDEJhbUZhbm"
    + "N5RW51bRIUChBNWV9BV0VTT01FX0ZJRUxEEABiBnByb3RvMw=="
)

# Created using protoc
schema_protobuf_nested_message4_bin_protoc = (
    "CiN0ZXN0cy9zY2hlbWFzL25lc3RlZF9tZXNzYWdlNC5wcm90bxIZZmFuY3kuY29tcGFueS"
    "5pbi5wYXJ0eS52MSL5AQoOQW5vdGhlck1lc3NhZ2Ua5gEKEVdvd0FOZXN0ZWRNZXNzYWdl"
    "GqoBCg9EZWVwbHlOZXN0ZWRNc2calgEKFUFub3RoZXJMZXZlbE9mTmVzdGluZxJ9CitpbV"
    "90cmlja3lfaW1fcmVmZXJyaW5nX3RvX3RoZV9wcmV2aW91c19lbnVtGAEgASgOMkguZmFu"
    "Y3kuY29tcGFueS5pbi5wYXJ0eS52MS5Bbm90aGVyTWVzc2FnZS5Xb3dBTmVzdGVkTWVzc2"
    "FnZS5CYW1GYW5jeUVudW0iJAoMQmFtRmFuY3lFbnVtEhQKEE1ZX0FXRVNPTUVfRklFTEQQ"
    "AGIGcHJvdG8z"
)

schema_protobuf_oneof = """\
syntax = "proto3";

message Goods {
  float ff = 5;

  oneof item {
    string name_a = 1;
    string name_b = 2;
    int32 id = 3;
    double dd = 4;
  }
  oneof item2 {
    uint64 ui = 6;
    bool bbb = 7;
    sfixed32 sf = 32;
    bytes bye = 33;
    sint64 sintti = 42;
  }
}
"""

schema_protobuf_oneof_bin = (
    "CgdkZWZhdWx0Iq4BCgVHb29kcxIQCgZuYW1lX2EYASABKAlIABIQCgZuYW1lX2IYAiABKA"
    + "lIABIMCgJpZBgDIAEoBUgAEgwKAmRkGAQgASgBSAASDAoCdWkYBiABKARIARINCgNiYm"
    + "IYByABKAhIARIMCgJzZhggIAEoD0gBEg0KA2J5ZRghIAEoDEgBEhAKBnNpbnR0aRgqIA"
    + "EoEkgBEggKAmZmGAUoAkIGCgRpdGVtQgcKBWl0ZW0yYgZwcm90bzM="
)


schema_protobuf_container2 = """\
syntax = "proto3";
package a1;

message container {
  message H {
    int32 s = 1;
  }
}
"""

schema_protobuf_container2_bin = "CgdkZWZhdWx0EgJhMSIZCgljb250YWluZXIaDAoBSBIHCgFzGAEoBWIGcHJvdG8z"

schema_protobuf_references = """\
syntax = "proto3";
package a1;

import "container2.proto";

message TestMessage {
  string t = 1;
  .a1.TestMessage.V v = 2;

  message V {
    .a1.container.H h = 1;
    int32 x = 2;
  }
}
"""

schema_protobuf_references_bin = (
    "CgdkZWZhdWx0EgJhMRoQY29udGFpbmVyMi5wcm90byJWCgtUZXN0TWVzc2FnZRIHCgF0GA"
    + "EoCRIYCgF2GAIyES5hMS5UZXN0TWVzc2FnZS5WGiQKAVYSFgoBaBgBMg8uYTEuY29udG"
    + "FpbmVyLkgSBwoBeBgCKAViBnByb3RvMw=="
)

schema_protobuf_references2 = """\
syntax = "proto3";
package a1;

import public "container2.proto";

message TestMessage {
  string t = 1;
  .a1.TestMessage.V v = 2;

  message V {
    .a1.container.H h = 1;
    int32 x = 2;
  }
}
"""

schema_protobuf_references2_bin = (
    "CgdkZWZhdWx0EgJhMRoQY29udGFpbmVyMi5wcm90byJWCgtUZXN0TWVzc2FnZRIHCgF0GA"
    + "EoCRIYCgF2GAIyES5hMS5UZXN0TWVzc2FnZS5WGiQKAVYSFgoBaBgBMg8uYTEuY29udG"
    + "FpbmVyLkgSBwoBeBgCKAVQAGIGcHJvdG8z"
)

schema_protobuf_complex = """\
import "google/protobuf/descriptor.proto";

message Foo {
  reserved 10, 12 to 14, "foo";
}
enum Data {
  DATA_UNSPECIFIED = 0;
  DATA_SEARCH = 1 [deprecated = true];
  DATA_DISPLAY = 2;
}
"""

schema_protobuf_complex_bin = (
    "CgdkZWZhdWx0GiBnb29nbGUvcHJvdG9idWYvZGVzY3JpcHRvci5wcm90byIWCgNGb29KBA"
    + "gKEAtKBAgMEA9SA2ZvbypDCgREYXRhEhQKEERBVEFfVU5TUEVDSUZJRUQQABITCgtEQV"
    + "RBX1NFQVJDSBABGgIIARIQCgxEQVRBX0RJU1BMQVkQAg=="
)

schema_protobuf_nested_field = """\
syntax = "proto3";

message Register {
  .Register.Metadata metadata = 1;
  string company_number = 2;

  message Metadata {
    string id = 1;
  }
}"""

schema_protobuf_nested_field_bin_protoc = (
    "Cg5yZWdpc3Rlci5wcm90byJgCghSZWdpc3RlchIkCghtZXRhZGF0YRgBIAEoCzISLlJlZ2"
    "lzdGVyLk1ldGFkYXRhEhYKDmNvbXBhbnlfbnVtYmVyGAIgASgJGhYKCE1ldGFkYXRhEgoK"
    "AmlkGAEgASgJYgZwcm90bzM="
)

schema_protobuf_optionals_bin = (
    "Cgp0ZXN0LnByb3RvIqYBCgpEaW1lbnNpb25zEhEKBHNpemUYASABKAFIAIgBARISCgV3aWR0aBgCIAEoAUgBiAEBEhMKBmhlaWdodBgDIAEo"
    + "AUgCiAEBEhMKBmxlbmd0aBgEIAEoAUgDiAEBEhMKBndlaWdodBgFIAEoAUgEiAEBQgcKBV9zaXplQggKBl93aWR0aEIJCgdfaGVpZ2h0Qg"
    + "kKB19sZW5ndGhCCQoHX3dlaWdodGIGcHJvdG8z"
)

schema_protobuf_optionals = """\
syntax = "proto3";

message Dimensions {
  optional double size = 1;
  optional double width = 2;
  optional double height = 3;
  optional double length = 4;
  optional double weight = 5;
}
"""
