package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Message struct {
	ID        string
	Payload   string
	Timestamp int64
}

type QueueConfig struct {
	DLQ        bool
	MaxRetries int8
}

type Queue struct {
	Name     string
	Config   QueueConfig
	Messages []*Message
	Mutex    sync.Mutex
}

type PendingAck struct {
	Message  *Message
	TimeSent time.Time
}
type FileStorage struct {
	MessagesPath string
	StatePath    string
}

type BrokerState struct {
	Exchanges   map[string]string              `json:"exchanges"`
	Queues      map[string][]string            `json:"queues"`
	Bindings    map[string]map[string][]string `json:"bindings"`
	PendingAcks map[string][]string            `json:"pending_acks"`
}

type BrokerMetadata struct {
	QueueConfigs   map[string]QueueConfig `json:"queue_configs"`
	SchemaRegistry map[string]string      `json:"schema_registry"`
}

func (q *Queue) enqueue(msg *Message) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Messages = append(q.Messages, msg)
	log.Println(msg.ID, "message enqueued")
}

func (q *Queue) dequeue() *Message {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	if len(q.Messages) == 0 {
		return nil
	}

	msg := q.Messages[0]
	q.Messages = q.Messages[1:]
	log.Println(msg.ID, "message dequeued")
	return msg
}

func (q *Queue) peek(n int) []*Message {
	if n > len(q.Messages) {
		n = len(q.Messages)
	}

	return q.Messages[:n]
}

func (q *Queue) size() int {
	return len(q.Messages)
}

func newFileStorage(statePath, messagesPath string) (*FileStorage, error) {
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

func (f *FileStorage) storeBrokerMetadata(metadata []byte) error {
	path := filepath.Join(f.StatePath, fmt.Sprintf("%s.json", "metadata"))
	return os.WriteFile(path, metadata, 0644)
}

func (f *FileStorage) getBrokerMetadata() (*BrokerMetadata, error) {
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

func (f *FileStorage) storeBrokerState(brokerState []byte) error {
	path := filepath.Join(f.StatePath, fmt.Sprintf("%s.json", "state"))
	return os.WriteFile(path, brokerState, 0644)
}

func (f *FileStorage) getBrokerState() (*BrokerState, error) {
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

func (f *FileStorage) storeMessage(msg *Message) error {
	path := filepath.Join(f.MessagesPath, fmt.Sprintf("%s.json", msg.ID))

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (f *FileStorage) getMessage(msgID string) (*Message, error) {
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
