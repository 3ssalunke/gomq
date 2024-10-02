package util

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"google.golang.org/protobuf/proto" // Import for proto.Unmarshal
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Function to generate Protobuf descriptor from schema string
func generateDescriptor(schema string) (*descriptorpb.FileDescriptorSet, error) {
	// Step 1: Write the schema string to a temporary .proto file
	tmpFile, err := ioutil.TempFile("", "*.proto")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up the file after validation

	if _, err := tmpFile.WriteString(schema); err != nil {
		return nil, fmt.Errorf("failed to write schema to temp file: %v", err)
	}
	tmpFile.Close()

	// Get the directory of the temporary file
	protoDir := filepath.Dir(tmpFile.Name())

	// Step 2: Create a temporary output file for the descriptor set
	outputFile, err := ioutil.TempFile("", "*.bin")
	if err != nil {
		return nil, fmt.Errorf("failed to create output temp file: %v", err)
	}
	defer os.Remove(outputFile.Name()) // Clean up the output file after validation

	// Step 3: Run the protoc command to generate the descriptor
	cmd := exec.Command("protoc", "--proto_path="+protoDir, tmpFile.Name(), "--descriptor_set_out="+outputFile.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr // Capture standard error
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("protoc validation failed: %v: %s", err, stderr.String())
	}

	// Step 4: Read and unmarshal the descriptor set
	descriptorData, err := ioutil.ReadFile(outputFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read descriptor file: %v", err)
	}
	var fileDescriptorSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(descriptorData, &fileDescriptorSet); err != nil {
		return nil, fmt.Errorf("failed to unmarshal descriptor set: %v", err)
	}

	return &fileDescriptorSet, nil
}

func UnmarshalBytesToProtobuf(messageBytes []byte, exchange, schema string) error {
	// Step 1: Generate the descriptor set from the schema string
	fileDescriptorSet, err := generateDescriptor(schema)
	if err != nil {
		return fmt.Errorf("failed to generate descriptor: %v", err)
	}

	// Step 2: Convert the descriptor set to a FileDescriptor
	fileDescriptor, err := protodesc.NewFile(fileDescriptorSet.File[0], nil)
	if err != nil {
		return fmt.Errorf("failed to create file descriptor: %v", err)
	}

	// Step 3: Get the message descriptor from the descriptor set
	messageDescriptor := fileDescriptor.Messages().ByName(protoreflect.Name(exchange)) // Replace with the correct message name
	if messageDescriptor == nil {
		return fmt.Errorf("message 'User' not found in schema")
	}

	// Step 4: Create a dynamic message using the message descriptor
	dynamicMessage := dynamicpb.NewMessage(messageDescriptor)

	// Step 5: Unmarshal the byte array into the dynamic message
	if err := proto.Unmarshal(messageBytes, dynamicMessage); err != nil {
		return fmt.Errorf("failed to unmarshal bytes into Protobuf message: %v", err)
	}

	// Print the dynamic message (for demonstration)
	fmt.Printf("Dynamic Message: %v\n", dynamicMessage)
	return nil
}
