package broker

import (
	"fmt"
	"log"
	"sync"

	"github.com/3ssalunke/gomq/internal/util"
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

func (b *Broker) CreateExchange(name, exchangeType string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if util.MapContains(b.exchanges, name) {
		log.Printf("exchange with the name %s already exists", name)
		return fmt.Errorf("exchange with the name %s already exists", name)
	}

	b.exchanges[name] = exchangeType
	log.Printf("exchange %s of type %s created", name, exchangeType)
	return nil
}

func (b *Broker) CreateQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if util.MapContains(b.queues, name) {
		log.Printf("queue with the name %s already exists", name)
		return fmt.Errorf("queue with the name %s already exists", name)
	}

	b.queues[name] = &Queue{Name: name, Messages: make([]*Message, 0)}
	log.Printf("queue %s created", name)
	return nil
}

func (b *Broker) BindQueue(exchange, queue, routing_key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.bindings[exchange] == nil {
		b.bindings[exchange] = make(map[string][]string)
	}

	if util.SliceContains(b.bindings[exchange][queue], routing_key) {
		log.Printf("queue %s is already binded to exchange %s with the routing key %s", queue, exchange, routing_key)
		return fmt.Errorf("queue %s is already binded to exchange %s with the routing key %s", queue, exchange, routing_key)
	}

	b.bindings[exchange][queue] = append(b.bindings[exchange][queue], routing_key)
	log.Printf("queue %s is binded to exchange %s with the routing key %s", queue, exchange, routing_key)
	return nil
}

func (b *Broker) PublishMessage(exchange, routing_key string, msg *Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.bindings, exchange) {
		log.Printf("no bindings exist for exchange %s", exchange)
		return fmt.Errorf("no bindings exist for exchange %s", exchange)
	}

	for queue, keys := range b.bindings[exchange] {
		for _, key := range keys {
			if key == routing_key {
				b.queues[queue].Enqueue(msg)
				return nil
			}
		}
	}

	log.Printf("no queue is binded to exchange %s by routing key %s", exchange, routing_key)
	return fmt.Errorf("no queue is binded to exchange %s by routing key %s", exchange, routing_key)
}

func (b *Broker) ConsumeMessage(queueName string) (*Message, error) {
	if util.MapContains(b.queues, queueName) {
		return b.queues[queueName].Dequeue(), nil
	}

	log.Printf("queue %s does not exist", queueName)
	return nil, fmt.Errorf("queue %s does not exist", queueName)
}

func (b *Broker) RetrieveMessages(queueName string, n int) ([]*Message, error) {
	if util.MapContains(b.queues, queueName) {
		return b.queues[queueName].Peek(n), nil
	}

	log.Printf("queue %s does not exist", queueName)
	return nil, fmt.Errorf("queue %s does not exist", queueName)
}
