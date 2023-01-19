"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
schema_protobuf_plain = """syntax = "proto3";
package com.codingharbour.protobuf;

option java_outer_classname = "SimpleMessageProtos";
message SimpleMessage {
  string content = 1;
  string date_time = 2;
  string content2 = 3;
}
"""

schema_protobuf_schema_registry1 = """
|syntax = "proto3";
|package com.codingharbour.protobuf;
|
|message SimpleMessage {
|  string content = 1;
|  string my_string = 2;
|  int32 my_int = 3;
|}
|
"""

schema_protobuf_order_before = """
|syntax = "proto3";
|
|option java_package = "com.codingharbour.protobuf";
|option java_outer_classname = "TestEnumOrder";
|
|enum Enum {
|  HIGH = 0;
|  MIDDLE = 1;
|  LOW = 2;
|}
|message Message {
|  int32 query = 1;
|}
"""

schema_protobuf_order_after = """
|syntax = "proto3";
|
|option java_package = "com.codingharbour.protobuf";
|option java_outer_classname = "TestEnumOrder";
|
|message Message {
|  int32 query = 1;
|}
|enum Enum {
|  HIGH = 0;
|  MIDDLE = 1;
|  LOW = 2;
|}
|
"""

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
