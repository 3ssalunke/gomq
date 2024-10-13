package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/3ssalunke/gomq/broker/internal/auth"
)

type FileStorage struct {
	MessagesPath string
	StatePath    string
}

type BrokerState struct {
	Exchanges   map[string]ExchangeType        `json:"exchanges"`
	Queues      map[string][]string            `json:"queues"`
	Bindings    map[string]map[string][]string `json:"bindings"`
	PendingAcks map[string][]string            `json:"pending_acks"`
}

type AuthStore struct {
	Admin auth.User
	Users map[string]auth.User
}

type BrokerMetadata struct {
	QueueConfigs   map[string]QueueConfig `json:"queue_configs"`
	SchemaRegistry map[string]string      `json:"schema_registry"`
	Auth           AuthStore              `json:"auth_store"`
}

func NewFileStorage(statePath, messagesPath string) (*FileStorage, error) {
	_, err := os.Stat(statePath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(statePath, os.ModePerm); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	_, err = os.Stat(messagesPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(messagesPath, os.ModePerm); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	return &FileStorage{
		StatePath:    statePath,
		MessagesPath: messagesPath,
	}, nil
}

func (f *FileStorage) StoreBrokerMetadata(metadata []byte) error {
	path := filepath.Join(f.StatePath, fmt.Sprintf("%s.json", "metadata"))
	return os.WriteFile(path, metadata, 0644)
}

func (f *FileStorage) GetBrokerMetadata() (*BrokerMetadata, error) {
	path := filepath.Join(f.StatePath, fmt.Sprintf("%s.json", "metadata"))

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var brokerMetadata *BrokerMetadata
	err = json.Unmarshal(byteValue, &brokerMetadata)
	if err != nil {
		return nil, err
	}

	return brokerMetadata, nil
}

func (f *FileStorage) StoreBrokerState(brokerState []byte) error {
	path := filepath.Join(f.StatePath, fmt.Sprintf("%s.json", "state"))
	return os.WriteFile(path, brokerState, 0644)
}

func (f *FileStorage) GetBrokerState() (*BrokerState, error) {
	path := filepath.Join(f.StatePath, fmt.Sprintf("%s.json", "state"))

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var brokerState *BrokerState
	err = json.Unmarshal(byteValue, &brokerState)
	if err != nil {
		return nil, err
	}

	return brokerState, nil
}

func (f *FileStorage) StoreMessage(msg *Message) error {
	path := filepath.Join(f.MessagesPath, fmt.Sprintf("%s.json", msg.ID))

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (f *FileStorage) GetMessage(msgID string) (*Message, error) {
	path := filepath.Join(f.MessagesPath, fmt.Sprintf("%s.json", msgID))
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var message *Message
	err = json.Unmarshal(byteValue, &message)
	if err != nil {
		return nil, err
	}

	return message, nil
}
