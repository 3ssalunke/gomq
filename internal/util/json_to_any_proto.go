package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"google.golang.org/protobuf/proto" // Import for proto.Unmarshal
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
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

// Function to convert JSON to Protobuf message using the descriptor
func ConvertJSONToProto(jsonString, messageName, schema string) (*anypb.Any, error) {
	// Step 1: Generate the descriptor set from schema string
	fileDescriptorSet, err := generateDescriptor(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to generate descriptor: %v", err)
	}

	// Step 2: Get the message descriptor from the descriptor set
	fileDescriptor, err := protodesc.NewFile(fileDescriptorSet.File[0], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptor: %v", err)
	}

	name := protoreflect.Name(messageName)

	// Assuming the message name is "User" (change accordingly)
	messageDescriptor := fileDescriptor.Messages().ByName(name)
	if messageDescriptor == nil {
		return nil, fmt.Errorf("message %s not found in schema", messageName)
	}

	// Step 3: Create a dynamic message using the message descriptor
	log.Println("messageDescriptor--->", messageDescriptor)
	dynamicMessage := dynamicpb.NewMessage(messageDescriptor)

	// Step 4: Unmarshal the JSON into the dynamic message
	if err := json.Unmarshal([]byte(jsonString), dynamicMessage.Interface()); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to dynamic message: %v", err)
	}
	log.Println("dynamicMessage--->", dynamicMessage)

	// Check if the message has valid data
	if dynamicMessage.IsValid() {
		// Step 5: Convert the dynamic message to Protobuf Any type
		anyMsg, err := anypb.New(dynamicMessage)
		log.Println("anyMsg--->", anyMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to Any: %v", err)
		}
		return anyMsg, nil
	}

	return nil, fmt.Errorf("dynamic message is empty or invalid")
}
