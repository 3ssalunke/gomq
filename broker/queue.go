package broker

import (
	"log"
	"sync"
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

func (q *Queue) Enqueue(msg *Message) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Messages = append(q.Messages, msg)
	log.Println("message enqueued")
}

func (q *Queue) Dequeue() *Message {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	if len(q.Messages) == 0 {
		return nil
	}

	msg := q.Messages[0]
	q.Messages = q.Messages[1:]
	log.Println("message dequeued")
	return msg
}
