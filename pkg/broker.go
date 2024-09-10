package pkg

import "sync"

type Broker struct {
	Queues map[string]*Queue
	Mutex  sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		Queues: make(map[string]*Queue),
	}
}

func (b *Broker) CreateQueue(name string) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	b.Queues[name] = &Queue{Name: name, Messages: make([]*Message, 0)}
}

func (b *Broker) Publish(queueName string, msg *Message) {
	if queue, ok := b.Queues[queueName]; ok {
		queue.Enqueue(msg)
	}
}

func (b *Broker) Subscribe(queueName string) *Message {
	if queue, ok := b.Queues[queueName]; ok {
		return queue.Dequeue()
	}
	return nil
}
