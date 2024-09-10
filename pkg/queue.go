package pkg

import "sync"

type Message struct {
	ID        string
	Payload   []byte
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
}

func (q *Queue) Dequeue() *Message {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	if len(q.Messages) == 0 {
		return nil
	}

	msg := q.Messages[0]
	q.Messages = q.Messages[1:]
	return msg
}
