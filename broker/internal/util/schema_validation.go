package util

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

func ValidateProtobufSchema(schema string) error {
	// Write the schema string to a temporary .proto file
	tmpFile, err := ioutil.TempFile("", "*.proto")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up the file after validation

	if _, err := tmpFile.WriteString(schema); err != nil {
		return fmt.Errorf("failed to write schema to temp file: %v", err)
	}
	tmpFile.Close()

	// Get the directory of the temporary file
	protoDir := filepath.Dir(tmpFile.Name())

	// Create a temporary output file to satisfy protoc requirements
	outputFile, err := ioutil.TempFile("", "*.bin")
	if err != nil {
		return fmt.Errorf("failed to create output temp file: %v", err)
	}
	defer os.Remove(outputFile.Name()) // Clean up the output file after validation

	// Run the protoc command to validate the .proto file
	cmd := exec.Command("protoc", "--proto_path="+protoDir, tmpFile.Name(), "--descriptor_set_out="+outputFile.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr // Capture standard error

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("protoc validation failed: %v: %s", err, stderr.String())
	}

	// If no error, schema is valid
	return nil
}
