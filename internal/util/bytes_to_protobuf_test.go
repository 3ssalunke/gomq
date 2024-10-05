package util

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestGenerateDescriptor_ValidSchema tests generating a descriptor for a valid schema.
func TestGenerateDescriptor_ValidSchema(t *testing.T) {
	schema := `
		syntax = "proto3";
		message User {
			string name = 1;
			int32 age = 2;
		}
	`

	// Call the function
	descriptor, err := generateDescriptor(schema, "")

	// Assertions
	require.NoError(t, err, "Expected no error for valid schema")
	require.NotNil(t, descriptor, "Expected descriptor to be generated")
}

// TestGenerateDescriptor_InvalidSchema tests generating a descriptor for an invalid schema.
func TestGenerateDescriptor_InvalidSchema(t *testing.T) {
	schema := `
		syntax = "proto3";
		message User {
			string name = 1;
			invalid_type age = 2;  // Invalid type
		}
	`

	// Call the function
	_, err := generateDescriptor(schema, "")

	// Assertions
	require.Error(t, err, "Expected error for invalid schema")
}

// TestUnmarshalBytesToProtobuf_ValidMessage tests unmarshalling a valid byte array.
func TestUnmarshalBytesToProtobuf_ValidMessage(t *testing.T) {
	schema := `
		syntax = "proto3";
		message Person {
			string name = 1;
			int32 id = 2;
			string email = 3;
		}
	`

	// Create a valid Protobuf message (User)
	user := &Person{
		Name:  "Alice",
		Id:    int32(GenerateRandomInt()),
		Email: "Alice@test.com",
	}

	// Marshal the message to a byte array
	userBytes, err := proto.Marshal(user)
	require.NoError(t, err, "Failed to marshal test user message")

	// Call the unmarshal function
	err = UnmarshalBytesToProtobuf(userBytes, "Person", schema)

	// Assertions
	require.NoError(t, err, "Expected no error for valid Protobuf byte array")
}

// TestUnmarshalBytesToProtobuf_InvalidMessage tests unmarshalling an invalid byte array.
func TestUnmarshalBytesToProtobuf_InvalidMessage(t *testing.T) {
	schema := `
		syntax = "proto3";
		message User {
			string name = 1;
			int32 age = 2;
		}
	`

	// Simulate an invalid byte array (doesn't conform to the User schema)
	invalidBytes := []byte{0xFF, 0xFF, 0xFF}

	// Call the unmarshal function
	err := UnmarshalBytesToProtobuf(invalidBytes, "Person", schema)

	// Assertions
	require.Error(t, err, "Expected error for invalid byte array")
}
