package util

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func ValidateProtobufSchema(schema string) error {
	tmpFile, err := os.CreateTemp("", "*.proto")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(schema); err != nil {
		return fmt.Errorf("failed to write schema to temp file: %v", err)
	}
	tmpFile.Close()

	protoDir := filepath.Dir(tmpFile.Name())

	outputFile, err := os.CreateTemp("", "*.bin")
	if err != nil {
		return fmt.Errorf("failed to create output temp file: %v", err)
	}
	defer os.Remove(outputFile.Name())

	cmd := exec.Command("protoc", "--proto_path="+protoDir, tmpFile.Name(), "--descriptor_set_out="+outputFile.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("protoc validation failed: %v: %s", err, stderr.String())
	}

	return nil
}
