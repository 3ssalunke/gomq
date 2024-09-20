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
	Path string
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

func (f *FileStorage) createBindRouteStore(routingKey string) error {
	dir := filepath.Join(f.Path, routingKey)

	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
}

func (f *FileStorage) storeMessage(queueName string, msg *Message) error {
	path := filepath.Join(f.Path, queueName, fmt.Sprintf("%s.json", msg.ID))

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
