package broker

import (
	"log"
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
