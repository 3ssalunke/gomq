package util

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/3ssalunke/gomq/shared/util"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	DescriptorDir = "./descriptors"
)

func StoreDescriptorToFile(schema, protoFileName string) error {
	fileDescriptorSet, err := util.GenerateDescriptor(schema, protoFileName)
	if err != nil {
		return fmt.Errorf("failed to generate descriptor: %v", err)
	}

	descriptorData, err := proto.Marshal(fileDescriptorSet)
	if err != nil {
		return fmt.Errorf("failed to marshal descriptor %v", err)
	}

	_, err = os.Stat(DescriptorDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(DescriptorDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create descriptors dir: %v", err)
		}
	}

	descriptorFilePath := filepath.Join(DescriptorDir, protoFileName+".bin")
	file, err := os.Create(descriptorFilePath)
	if err != nil {
		return fmt.Errorf("failed to create descriptor file: %v", err)
	}

	_, err = file.Write(descriptorData)

	if err != nil {
		return fmt.Errorf("failed to write descriptor to file %v", err)
	}

	return nil
}

func LoadDescriptorFromFile(descriptorFileName string) (protoreflect.FileDescriptor, error) {
	descriptorFilePath := filepath.Join(DescriptorDir, descriptorFileName)
	descriptorData, err := os.ReadFile(descriptorFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read descriptor file %v", err)
	}

	var fdSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(descriptorData, &fdSet); err != nil {
		return nil, fmt.Errorf("failed to unmarshal descriptor set: %v", err)
	}

	fd, err := protodesc.NewFile(fdSet.File[0], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create FileDescriptor: %v", err)
	}

	return fd, nil
}
