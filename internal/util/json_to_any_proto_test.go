package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test with valid schema and valid JSON
func TestConvertJSONToProto_ValidInput(t *testing.T) {
	protoSchema := `
		syntax = "proto3";
		message User {
			string name = 1;
			int32 age = 2;
		}
	`
	jsonString := `{"name": "Alice", "age": 30}`

	// Call the function
	result, err := ConvertJSONToProto(jsonString, "User", protoSchema)

	// Assertions
	require.NoError(t, err, "Expected no error for valid input")
	assert.NotNil(t, result, "Expected result to be non-nil")
}

// Test with invalid schema
func TestConvertJSONToProto_InvalidSchema(t *testing.T) {
	invalidSchema := `
		syntax = "proto3";
		message User {
			string name = 1;
			int32 age = 2;
			invalid_field float; // Invalid field type, expected int32, float32, etc.
		}
	`
	jsonString := `{"name": "Alice", "age": 30}`

	// Call the function
	_, err := ConvertJSONToProto(jsonString, "User", invalidSchema)

	// Assertions
	require.Error(t, err, "Expected an error for invalid schema")
	assert.Contains(t, err.Error(), "protoc validation failed", "Expected protoc validation failure")
}

// Test with valid schema but invalid JSON
// func TestConvertJSONToProto_InvalidJSON(t *testing.T) {
// 	protoSchema := `
// 		syntax = "proto3";
// 		message User {
// 			string name = 1;
// 			int32 age = 2;
// 		}
// 	`
// 	invalidJSON := `{"name": "Alice", "age": "thirty"}` // "age" should be an integer

// 	// Call the function
// 	_, err := ConvertJSONToProto(invalidJSON, "User", protoSchema)

// 	// Assertions
// 	require.Error(t, err, "Expected an error for invalid JSON")
// 	assert.Contains(t, err.Error(), "failed to unmarshal JSON to dynamic message", "Expected JSON unmarshaling error")
// }

// Test with missing message name in schema
func TestConvertJSONToProto_MissingMessageName(t *testing.T) {
	protoSchema := `
		syntax = "proto3";
		message Person {
			string name = 1;
			int32 age = 2;
		}
	`
	jsonString := `{"name": "Alice", "age": 30}`

	// The schema defines "Person", but we're looking for "User"
	_, err := ConvertJSONToProto(jsonString, "User", protoSchema)

	// Assertions
	require.Error(t, err, "Expected an error for missing message")
	assert.Contains(t, err.Error(), "message User not found in schema", "Expected error for missing message 'User'")
}

// Test with empty schema and JSON
func TestConvertJSONToProto_EmptyInputs(t *testing.T) {
	emptySchema := ``
	emptyJSON := `{}`

	// Call the function
	_, err := ConvertJSONToProto(emptyJSON, "User", emptySchema)

	// Assertions
	require.Error(t, err, "Expected an error for empty inputs")
}
