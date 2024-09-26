package main

import (
	"testing"

	s "github.com/Aiven-Open/karapace/go/protopace/schema"
	"github.com/stretchr/testify/assert"
)

func TestFormatEnum(t *testing.T) {

	testProto :=
		`syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    option (my_option3) = "my_value3";
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";

    ACTIVE = 0;
}`

	expected :=
		`syntax = "proto3";
package pkg;

import "google/protobuf/descriptor.proto";
extend google.protobuf.EnumOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}
enum MyEnum {
  option (my_option) = "my_value";
  option (my_option2) = "my_value2";
  option (my_option3) = "my_value3";
  ACTIVE = 0;
}
`

	assert := assert.New(t)

	testSchema, err := s.FromString("test.proto", testProto, []s.Schema{})
	assert.NoError(err)
	assert.NotNil(testSchema)

	result, err := Format(*testSchema)
	assert.NoError(err)
	assert.Equal(result.Schema, expected)
}

func TestFormatFullyQualifiedNames(t *testing.T) {

	dependency :=
		`
syntax = "proto3";
package my.awesome.customer.v1;

message NestedValue {
    string value = 1;
}
`

	dependencyV2 :=
		`
syntax = "proto3";
package my.awesome.customer.v2;

enum Status {
	ACTIVE = 0;
	INACTIVE = 1;
}

message NestedValue {
    string value = 1;
	Status status = 2;
}
`

	testProtoA :=
		`syntax = "proto3";
package my.awesome.customer.v2;

import "my/awesome/customer/v1/nested_value.proto";
import "my/awesome/customer/v2/nested_value.proto";
import "google/protobuf/timestamp.proto";

option objc_class_prefix = "TDD";
option php_metadata_namespace = "My\\\\Awesome\\\\Customer\\\\V1";
option php_namespace = "My\\\\Awesome\\\\Customer\\\\V1";
option ruby_package = "My::Awesome::Customer::V1";

option csharp_namespace = "my.awesome.customer.V1";
option go_package = "github.com/customer/api/my/awesome/customer/v1;dspv1";
option java_multiple_files = true;
option java_outer_classname = "EventValueProto";
option java_package = "com.my.awesome.customer.v1";


message EventValue {
  .my.awesome.customer.v2.NestedValue nested_value = 1;
  .google.protobuf.Timestamp created_at = 2;


  .my.awesome.customer.v2.Status status = 3;
}
`

	testProtoB :=
		`syntax = "proto3";
package my.awesome.customer.v2;

import "my/awesome/customer/v1/nested_value.proto";
import "my/awesome/customer/v2/nested_value.proto";
import "google/protobuf/timestamp.proto";

option csharp_namespace = "my.awesome.customer.V1";
option go_package = "github.com/customer/api/my/awesome/customer/v1;dspv1";
option java_multiple_files = true;
option java_outer_classname = "EventValueProto";
option java_package = "com.my.awesome.customer.v1";
option objc_class_prefix = "TDD";
option php_metadata_namespace = "My\\\\Awesome\\\\Customer\\\\V1";
option php_namespace = "My\\\\Awesome\\\\Customer\\\\V1";
option ruby_package = "My::Awesome::Customer::V1";

message EventValue {
  NestedValue nested_value = 1;
  google.protobuf.Timestamp created_at = 2;
  Status status = 3;
}
`

	assert := assert.New(t)

	testDependency, err := s.FromString("my/awesome/customer/v1/nested_value.proto", dependency, []s.Schema{})
	assert.NoError(err)
	assert.NotNil(testDependency)

	testDependencyV2, err := s.FromString("my/awesome/customer/v2/nested_value.proto", dependencyV2, []s.Schema{})
	assert.NoError(err)
	assert.NotNil(testDependencyV2)

	testSchemaA, err := s.FromString("test.proto", testProtoA, []s.Schema{*testDependency, *testDependencyV2})
	assert.NoError(err)
	assert.NotNil(testSchemaA)
	a, err := Format(*testSchemaA)
	assert.NoError(err)

	testSchemaB, err := s.FromString("test.proto", testProtoB, []s.Schema{*testDependency, *testDependencyV2})
	assert.NoError(err)
	assert.NotNil(testSchemaB)
	b, err := Format(*testSchemaB)
	assert.NoError(err)

	assert.Equal(a.Schema, b.Schema)
	err = Check(b, a)
	assert.NoError(err)
}
