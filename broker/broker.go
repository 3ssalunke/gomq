package broker

import (
	"log"
	"sync"
)

type Broker struct {
	exchanges map[string]string
	queues    map[string]*Queue
	bindings  map[string]map[string][]string

	mu sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		exchanges: map[string]string{},
		queues:    make(map[string]*Queue),
		bindings:  map[string]map[string][]string{},
	}
}

func (b *Broker) CreateExchange(name, exchangeType string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.exchanges[name] = exchangeType
	log.Println("exchange created")
}

func (b *Broker) CreateQueue(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queues[name] = &Queue{Name: name, Messages: make([]*Message, 0)}
	log.Println("queue created")
}

func (b *Broker) BindQueue(exchange, queue, routing_key string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.bindings[exchange] == nil {
		b.bindings[exchange] = make(map[string][]string)
	}
	b.bindings[exchange][queue] = append(b.bindings[exchange][queue], routing_key)
	log.Println("queue binded")
}

func (b *Broker) Publish(exchange, routing_key string, msg *Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for queue, keys := range b.bindings[exchange] {
		for _, key := range keys {
			if key == routing_key {
				b.queues[queue].Enqueue(msg)
			}
		}
	}
}

func (b *Broker) Consume(queueName string) *Message {
	if queue, ok := b.queues[queueName]; ok {
		return queue.Dequeue()
	}
	return nil
}
