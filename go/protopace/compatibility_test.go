package main

import (
	"os"
	"testing"

	s "github.com/Aiven-Open/karapace/go/protopace/schema"
	"github.com/stretchr/testify/assert"
)

func TestCompatibility(t *testing.T) {
	assert := assert.New(t)

	data, _ := os.ReadFile("./fixtures/dependency.proto")
	dependencySchema, err := s.FromString("my/awesome/customer/v1/nested_value.proto", string(data), nil)
	assert.NoError(err)
	assert.NotNil(dependencySchema)

	data, _ = os.ReadFile("./fixtures/test.proto")
	testSchema, err := s.FromString("test.proto", string(data), []s.Schema{*dependencySchema})
	assert.NoError(err)
	assert.NotNil(testSchema)

	data, _ = os.ReadFile("./fixtures/test_previous.proto")
	previousSchema, err := s.FromString("test.proto", string(data), []s.Schema{*dependencySchema})
	assert.NoError(err)
	assert.NotNil(previousSchema)

	err = Check(*testSchema, *testSchema)
	assert.NoError(err)

	err = Check(*testSchema, *previousSchema)
	assert.ErrorContains(err, "Field \"5\" with name \"foo\" on message \"EventValue\" changed type from \"string\" to \"int32\".")
}
