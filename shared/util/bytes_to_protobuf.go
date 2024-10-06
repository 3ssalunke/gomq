package util

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/proto" // Import for proto.Unmarshal
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Function to generate Protobuf descriptor from schema string
func generateDescriptor(schema, filename string) (*descriptorpb.FileDescriptorSet, error) {
	var tmpFile *os.File
	if filename != "" {
		tmpFileName := filename + ".proto"

		var err error
		tmpFile, err = os.Create(tmpFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to create proto file: %v", err)
		}
	} else {
		var err error
		tmpFile, err = ioutil.TempFile("", "*.proto")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %v", err)
		}
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(schema); err != nil {
		return nil, fmt.Errorf("failed to write schema to temp file: %v", err)
	}
	tmpFile.Close()

	protoDir := filepath.Dir(tmpFile.Name())

	outputFile, err := ioutil.TempFile("", "*.bin")
	if err != nil {
		return nil, fmt.Errorf("failed to create output temp file: %v", err)
	}
	defer os.Remove(outputFile.Name())

	cmd := exec.Command("protoc", "--proto_path="+protoDir, tmpFile.Name(), "--descriptor_set_out="+outputFile.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("protoc validation failed: %v: %s", err, stderr.String())
	}

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

func RegisterDescriptorInRegistry(schema, filename string) error {
	fileDescriptorSet, err := generateDescriptor(schema, filename)
	if err != nil {
		return fmt.Errorf("failed to generate descriptor: %v", err)
	}

	for _, fdProto := range fileDescriptorSet.File {
		fd, err := protodesc.NewFile(fdProto, nil)
		if err != nil {
			return fmt.Errorf("failed to create FileDescriptor: %v", err)
		}

		if err := protoregistry.GlobalFiles.RegisterFile(fd); err != nil {
			return fmt.Errorf("failed to register FileDescriptor: %v", err)
		}

		fmt.Printf("Successfully registered: %s\n", fd.Path())
	}
	return nil
}

func UnmarshalBytesToProtobuf(messageBytes []byte, exchangeName, schema string) error {
	protoFileName := strings.ToLower(exchangeName) + ".proto"

	fd, err := protoregistry.GlobalFiles.FindFileByPath(protoFileName)
	if err != nil {
		log.Printf("error finding registered %s file: %v\n", protoFileName, err)

		if err := RegisterDescriptorInRegistry(schema, strings.ToLower(exchangeName)); err != nil {
			log.Printf("error registering descriptor: %v\n", err)
			return err
		}

		fd, err = protoregistry.GlobalFiles.FindFileByPath(protoFileName)
		if err != nil {
			log.Printf("error finding registered %s: %v\n", protoFileName, err)
			return err
		}
	}

	messageDescriptor := fd.Messages().ByName(protoreflect.Name(exchangeName))
	if messageDescriptor == nil {
		return fmt.Errorf("message 'User' not found in schema")
	}

	dynamicMessage := dynamicpb.NewMessage(messageDescriptor)

	if err := proto.Unmarshal(messageBytes, dynamicMessage); err != nil {
		return fmt.Errorf("failed to unmarshal bytes into Protobuf message: %v", err)
	}

	fmt.Printf("Dynamic Message: %v\n", dynamicMessage)
	return nil
}
