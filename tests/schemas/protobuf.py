schema_protobuf_plain = """
syntax = "proto3";
package com.codingharbour.protobuf;

option java_outer_classname = "SimpleMessageProtos";
message SimpleMessage {
  string content = 1;
  string date_time = 2;
  string content2 = 3;
}
"""
