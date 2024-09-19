package broker

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/3ssalunke/gomq/internal/util"
)

type Broker struct {
	ackTimeout  time.Duration
	exchanges   map[string]string
	queues      map[string]*Queue
	pendingAcks map[string]map[string]*PendingAck
	bindings    map[string]map[string][]string

	mu sync.Mutex
}

func NewBroker() *Broker {
	broker := &Broker{
		ackTimeout:  time.Minute * 1,
		exchanges:   make(map[string]string),
		queues:      make(map[string]*Queue),
		pendingAcks: make(map[string]map[string]*PendingAck),
		bindings:    make(map[string]map[string][]string),
	}

	go broker.clearUnackMessages()

	return broker
}

func (b *Broker) createExchange(name, exchangeType string) error {
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

func (b *Broker) createQueue(name string) error {
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

func (b *Broker) bindQueue(exchange, queue, routing_key string) error {
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

func (b *Broker) publishMessage(exchange, routing_key string, msg *Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.bindings, exchange) {
		log.Printf("no bindings exist for exchange %s", exchange)
		return fmt.Errorf("no bindings exist for exchange %s", exchange)
	}

	for queue, keys := range b.bindings[exchange] {
		for _, key := range keys {
			if key == routing_key {
				b.queues[queue].enqueue(msg)
				return nil
			}
		}
	}

	log.Printf("no queue is binded to exchange %s by routing key %s", exchange, routing_key)
	return fmt.Errorf("no queue is binded to exchange %s by routing key %s", exchange, routing_key)
}

func (b *Broker) consumeMessage(queueName string) (*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.queues, queueName) {
		log.Printf("queue %s does not exist", queueName)
		return nil, fmt.Errorf("queue %s does not exist", queueName)
	}

	message := b.queues[queueName].dequeue()

	if message != nil {
		if b.pendingAcks[queueName] == nil {
			b.pendingAcks[queueName] = make(map[string]*PendingAck)
		}
		b.pendingAcks[queueName][message.ID] = &PendingAck{
			Message:  message,
			TimeSent: time.Now(),
		}

		log.Printf("message %s is waiting for acknowdegement", message.ID)
	}

	return message, nil
}

func (b *Broker) messageAcknowledge(queueName, msgID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.pendingAcks, queueName) {
		log.Printf("queue %s does not exist", queueName)
		return fmt.Errorf("queue %s does not exist", queueName)
	}

	if !util.MapContains(b.pendingAcks[queueName], msgID) {
		log.Printf("message %s from queue %s does not have pending acknowledgement", msgID, queueName)
		return fmt.Errorf("message %s from queue %s does not have pending acknowledgement", msgID, queueName)
	}

	delete(b.pendingAcks[queueName], msgID)
	log.Printf("message %s has been acknowledged", msgID)

	return nil
}

func (b *Broker) clearUnackMessages() {
	for {
		log.Printf("clearing unacknowledged messages...")
		for queueName, messages := range b.pendingAcks {
			for messageID, pending := range messages {
				if time.Since(pending.TimeSent) > b.ackTimeout {
					b.queues[queueName].enqueue(pending.Message)
					delete(b.pendingAcks[queueName], messageID)

					log.Printf("message %s has not received acknowledgement and hence re-enqueued", messageID)
				}
			}
		}
		time.Sleep(time.Second * 10)
	}
}

func (b *Broker) retrieveMessages(queueName string, n int) ([]*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if util.MapContains(b.queues, queueName) {
		return b.queues[queueName].peek(n), nil
	}

	log.Printf("queue %s does not exist", queueName)
	return nil, fmt.Errorf("queue %s does not exist", queueName)
}
