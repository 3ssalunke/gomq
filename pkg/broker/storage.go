package broker

import (
	"encoding/json"
	"fmt"
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

type Queue struct {
	Name     string
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

func (f *FileStorage) storeBrokerState(brokerState []byte) error {
	path := filepath.Join(f.StatePath, fmt.Sprintf("%s.json", "state"))
	return os.WriteFile(path, brokerState, 0644)
}

func (f *FileStorage) storeMessage(msg *Message) error {
	path := filepath.Join(f.MessagesPath, fmt.Sprintf("%s.json", msg.ID))

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
