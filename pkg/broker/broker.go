package broker

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	fileStorage *FileStorage

	mu sync.Mutex
}

func NewBroker() *Broker {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("error while getting cwd", err)
	}
	storeagePath := filepath.Join(cwd, "store")

	broker := &Broker{
		ackTimeout:  time.Minute * 1,
		exchanges:   make(map[string]string),
		queues:      make(map[string]*Queue),
		pendingAcks: make(map[string]map[string]*PendingAck),
		bindings:    make(map[string]map[string][]string),
		fileStorage: &FileStorage{Path: storeagePath},
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

func (b *Broker) bindQueue(exchange, queue, routingKey string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.bindings[exchange] == nil {
		b.bindings[exchange] = make(map[string][]string)
	}

	if util.SliceContains(b.bindings[exchange][queue], routingKey) {
		log.Printf("queue %s is already binded to exchange %s with the routing key %s", queue, exchange, routingKey)
		return fmt.Errorf("queue %s is already binded to exchange %s with the routing key %s", queue, exchange, routingKey)
	}

	b.bindings[exchange][queue] = append(b.bindings[exchange][queue], routingKey)
	if err := b.fileStorage.createBindRouteStore(routingKey); err != nil {
		log.Printf("error while creating a store directory for route key %s", routingKey)
		return err
	}
	log.Printf("queue %s is binded to exchange %s with the routing key %s", queue, exchange, routingKey)
	return nil
}

func (b *Broker) publishMessage(exchange, routingKey string, msg *Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.bindings, exchange) {
		log.Printf("no bindings exist for exchange %s", exchange)
		return fmt.Errorf("no bindings exist for exchange %s", exchange)
	}

	if err := b.fileStorage.storeMessage(routingKey, msg); err != nil {
		log.Printf("error while writing message %s to store file", msg.ID)
		return err
	}

	for queue, keys := range b.bindings[exchange] {
		for _, key := range keys {
			if key == routingKey {
				b.queues[queue].enqueue(msg)
				log.Printf("route-queue %s-%s is enqueued with message %s", routingKey, queue, msg.ID)
			}
		}
	}

	return nil
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
		time.Sleep(time.Minute * 1)
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
