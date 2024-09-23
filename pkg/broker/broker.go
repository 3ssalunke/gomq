package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/3ssalunke/gomq/internal/util"
	"github.com/google/uuid"
)

type Consumer struct {
	ID      string
	MsgChan chan *Message
}

type ConsumersList struct {
	Consumers []*Consumer
	LastIndex int
}

type Broker struct {
	ackTimeout        time.Duration
	exchanges         map[string]string
	queues            map[string]*Queue
	pendingAcks       map[string]map[string]*PendingAck
	bindings          map[string]map[string][]string
	consumers         map[string]*ConsumersList
	stopConsumerChans map[string]chan bool
	fileStorage       *FileStorage

	mu sync.Mutex
}

func NewBroker() *Broker {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("error getting cwd", err)
	}
	statePath := filepath.Join(cwd, "store", "state")
	messagesPath := filepath.Join(cwd, "store", "messages")

	fileStorage, err := newFileStorage(statePath, messagesPath)
	if err != nil {
		log.Fatal("err creating broker store", err.Error())
	}

	broker := &Broker{
		ackTimeout:        time.Minute * 1,
		exchanges:         make(map[string]string),
		queues:            make(map[string]*Queue),
		pendingAcks:       make(map[string]map[string]*PendingAck),
		bindings:          make(map[string]map[string][]string),
		consumers:         make(map[string]*ConsumersList),
		stopConsumerChans: make(map[string]chan bool),
		fileStorage:       fileStorage,
	}

	if err := broker.restoreState(); err != nil {
		log.Fatal("error restoring broker state on startup", err.Error())
	}
	go broker.clearUnackMessages()

	return broker
}

func (b *Broker) saveState() error {
	queues := make(map[string][]string)
	for key, queue := range b.queues {
		messages := make([]string, 0)
		for _, message := range queue.Messages {
			messages = append(messages, message.ID)
		}
		queues[key] = messages
	}

	pendingAcks := make(map[string][]string)
	for queue, acks := range b.pendingAcks {
		msgIDs := make([]string, 0)
		for msgID := range acks {
			msgIDs = append(msgIDs, msgID)
		}
		pendingAcks[queue] = msgIDs
	}

	brokerJson, err := json.Marshal(BrokerState{
		Exchanges:   b.exchanges,
		Queues:      queues,
		Bindings:    b.bindings,
		PendingAcks: pendingAcks,
	})

	if err != nil {
		return err
	}

	if err = b.fileStorage.storeBrokerState(brokerJson); err != nil {
		return err
	}

	log.Println("broker state saved")
	return nil
}

func (b *Broker) restoreState() error {
	log.Println("restoring broker state on startup")
	brokerState, err := b.fileStorage.getBrokerState()
	if err != nil {
		return err
	}
	if brokerState == nil {
		log.Println("no broker state in store")
		return nil
	}

	queues := make(map[string]*Queue)
	pendingAcks := make(map[string]map[string]*PendingAck)

	for queueName, msgIDs := range brokerState.Queues {
		queue := &Queue{
			Name:     queueName,
			Messages: make([]*Message, 0),
			Mutex:    sync.Mutex{},
		}

		for _, msgID := range msgIDs {
			msg, err := b.fileStorage.getMessage(msgID)
			if err != nil {
				return err
			}
			queue.Messages = append(queue.Messages, msg)
		}

		queues[queueName] = queue
	}

	for queueName, msgIDs := range brokerState.PendingAcks {
		for _, msgID := range msgIDs {
			msg, err := b.fileStorage.getMessage(msgID)
			if err != nil {
				return err
			}
			if !util.MapContains(pendingAcks, queueName) {
				pendingAcks[queueName] = make(map[string]*PendingAck)
			}
			pendingAcks[queueName][msgID] = &PendingAck{
				Message:  msg,
				TimeSent: time.Now(),
			}
		}
	}

	b.exchanges = brokerState.Exchanges
	b.bindings = brokerState.Bindings
	b.queues = queues
	b.pendingAcks = pendingAcks

	return nil
}

func (b *Broker) createExchange(name, exchangeType string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if util.MapContains(b.exchanges, name) {
		log.Printf("exchange with the name %s already exists", name)
		return fmt.Errorf("exchange with the name %s already exists", name)
	}

	b.exchanges[name] = exchangeType
	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		delete(b.exchanges, name)
		return err
	}

	log.Printf("exchange %s of type %s created", name, exchangeType)
	return nil
}

func (b *Broker) removeExchange(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.exchanges, name) {
		log.Printf("exchange with the name %s does not exists", name)
		return fmt.Errorf("exchange with the name %s does not exists", name)
	}

	exchangeType := b.exchanges[name]
	var bindings map[string][]string
	var queues map[string]*Queue

	if util.MapContains(b.bindings, name) {
		bindings = b.bindings[name]
		for queueName := range bindings {
			if queues == nil {
				queues = map[string]*Queue{}
			}
			queues[queueName] = b.queues[queueName]
			delete(b.queues, queueName)
		}
	}

	delete(b.exchanges, name)
	delete(b.bindings, name)

	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		b.exchanges[name] = exchangeType
		b.bindings[name] = bindings
		for queueName, queue := range queues {
			b.queues[queueName] = queue
		}
		return err
	}

	log.Printf("exchange %s of type %s removed", name, exchangeType)
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
	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		delete(b.queues, name)
		return err
	}

	log.Printf("queue %s created", name)
	return nil
}

func (b *Broker) removeQueue(exchangeName, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.exchanges, exchangeName) {
		log.Printf("exchange with the name %s does not exist", exchangeName)
		return fmt.Errorf("exchange with the name %s does not exist", exchangeName)
	}

	if !util.MapContains(b.queues, queueName) {
		log.Printf("queue with the name %s does not exist", queueName)
		return fmt.Errorf("queue with the name %s does not exist", queueName)
	}

	queue := b.queues[queueName]
	bindings := b.bindings[exchangeName][queueName]

	delete(b.queues, queueName)
	delete(b.bindings[exchangeName], queueName)
	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		b.queues[queueName] = queue
		b.bindings[exchangeName][queueName] = bindings
		return err
	}

	log.Printf("queue %s created", queueName)
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
	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		b.bindings[exchange][queue] = b.bindings[exchange][queue][:len(b.bindings[exchange][queue])-1]
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

	for queue, keys := range b.bindings[exchange] {
		for _, key := range keys {
			if key == routingKey {
				b.queues[queue].enqueue(msg)
				log.Printf("route-queue %s-%s is enqueued with message %s", routingKey, queue, msg.ID)
			}
		}
	}

	if err := b.fileStorage.storeMessage(msg); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		for queue, keys := range b.bindings[exchange] {
			for _, key := range keys {
				if key == routingKey {
					b.queues[queue].dequeue()
				}
			}
		}
		return err
	}
	log.Printf("message %s saved to broker storage", msg.ID)

	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		for queue, keys := range b.bindings[exchange] {
			for _, key := range keys {
				if key == routingKey {
					b.queues[queue].dequeue()
				}
			}
		}
		return err
	}

	return nil
}

func (b *Broker) createConsumer(queueName string) (*Consumer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.queues, queueName) {
		log.Printf("queue %s does not exist", queueName)
		return nil, fmt.Errorf("queue %s does not exist", queueName)
	}

	if util.MapContains(b.stopConsumerChans, queueName) {
		b.stopConsumerChans[queueName] <- true
	}

	consumerID := uuid.New().String()
	consumer := &Consumer{
		ID:      consumerID,
		MsgChan: make(chan *Message),
	}

	if !util.MapContains(b.consumers, queueName) {
		b.consumers[queueName] = &ConsumersList{
			LastIndex: 0,
			Consumers: make([]*Consumer, 0),
		}
	}

	b.consumers[queueName] = &ConsumersList{
		LastIndex: 0,
		Consumers: append(b.consumers[queueName].Consumers, consumer),
	}
	stopChan := make(chan bool)
	b.stopConsumerChans[queueName] = stopChan

	go b.consumeMessage(queueName, stopChan)

	return consumer, nil
}

func (b *Broker) consumeMessage(queueName string, stopChan chan bool) {
	for {
		select {
		case <-stopChan:
			return
		default:
			message := b.queues[queueName].dequeue()

			if message != nil {
				if b.pendingAcks[queueName] == nil {
					b.pendingAcks[queueName] = make(map[string]*PendingAck)
				}
				b.pendingAcks[queueName][message.ID] = &PendingAck{
					Message:  message,
					TimeSent: time.Now(),
				}

				if err := b.saveState(); err != nil {
					log.Printf("error saving broker state %s, restroing broker state...", err.Error())

					b.queues[queueName].enqueue(message)
					delete(b.pendingAcks[queueName], message.ID)

					continue
				}

				lastIndex := b.consumers[queueName].LastIndex
				nextIndex := (lastIndex + 1) % len(b.consumers[queueName].Consumers)

				consumer := b.consumers[queueName].Consumers[nextIndex]
				consumer.MsgChan <- message

				b.consumers[queueName].LastIndex = nextIndex

				log.Printf("message %s is waiting for acknowdegement", message.ID)
			}

		}
	}
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

	pendingAck := b.pendingAcks[queueName][msgID]
	delete(b.pendingAcks[queueName], msgID)

	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		b.pendingAcks[queueName][msgID] = pendingAck

		return err
	}

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
		if err := b.saveState(); err != nil {
			log.Printf("error saving broker state %s", err.Error())
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
