package util

import "testing"

func TestValidateProtobufSchema(t *testing.T) {
	validSchmea := `syntax="proto3";message Person {string name = 1;int32 id = 2;string email = 3;}`
	invalidSchmea := `syntax="proto3";Person {string name = 1;int32 id = 2;string email = 3;}`

	t.Run("Valid schema should pass", func(t *testing.T) {
		err := ValidateProtobufSchema(validSchmea)
		if err != nil {
			t.Errorf("expected no error for valid schema, got %v", err)
		}
	})

	t.Run("Invalid schema should fail", func(t *testing.T) {
		err := ValidateProtobufSchema(invalidSchmea)
		if err == nil {
			t.Errorf("expected an error for invalid schema, but got nil")
		}
	})
}
