package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/3ssalunke/gomq/broker/internal/auth"
	"github.com/3ssalunke/gomq/broker/internal/config"
	"github.com/3ssalunke/gomq/broker/internal/storage"
	internalutil "github.com/3ssalunke/gomq/broker/internal/util"
	"github.com/3ssalunke/gomq/shared/pkg/protoc"
	"github.com/3ssalunke/gomq/shared/util"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	MAX_RETRIES = int8(3)
)

type Consumer struct {
	ID      string
	MsgChan chan *storage.Message
	Retries uint8
}

type ConsumersList struct {
	Consumers []*Consumer
	LastIndex int
}

type PeerConnection struct {
	conn   *grpc.ClientConn
	client protoc.ClusterSyncClient
}

type Broker struct {
	Config config.Config

	ackTimeout time.Duration

	exchanges      map[string]storage.ExchangeType
	schemaRegistry map[string]string
	queues         map[string]*storage.Queue
	bindings       map[string]map[string][]string

	pendingAcks map[string]map[string]*storage.PendingAck

	consumers         map[string]*ConsumersList
	stopConsumerChans map[string]chan bool

	messageRetries map[string]int8

	fileStorage *storage.FileStorage

	Auth *auth.Auth

	peerConnections []PeerConnection

	mu sync.Mutex
}

func NewBroker(config config.Config) *Broker {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("error getting cwd", err)
	}
	statePath := filepath.Join(cwd, config.BrokerStoreDir, config.BrokerStateDir)
	messagesPath := filepath.Join(cwd, config.BrokerStoreDir, config.BrokerMessagesDir)

	fileStorage, err := storage.NewFileStorage(statePath, messagesPath)
	if err != nil {
		log.Fatal("err creating broker store", err.Error())
	}

	auth := auth.NewAuth()

	broker := &Broker{
		Config:            config,
		ackTimeout:        time.Second * time.Duration(config.MessageAckTimeout),
		exchanges:         make(map[string]storage.ExchangeType),
		schemaRegistry:    make(map[string]string),
		queues:            make(map[string]*storage.Queue),
		pendingAcks:       make(map[string]map[string]*storage.PendingAck),
		bindings:          make(map[string]map[string][]string),
		consumers:         make(map[string]*ConsumersList),
		stopConsumerChans: make(map[string]chan bool),
		messageRetries:    make(map[string]int8),
		fileStorage:       fileStorage,
		Auth:              auth,
	}

	if config.IsMaster && len(broker.Config.PeerNodes) > 0 {
		for _, peerAddr := range broker.Config.PeerNodes {
			retries := 1
			var peerConnection PeerConnection
			for {
				if retries > 3 {
					log.Fatal("failed to make connection with peer node after several retries")
				}
				conn, err := grpc.NewClient(fmt.Sprintf("%s:50051", peerAddr), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err == nil {
					peerConnection = PeerConnection{
						conn:   conn,
						client: protoc.NewClusterSyncClient(conn),
					}
					break
				}
				time.Sleep(time.Duration(math.Pow(2, float64(retries))) * time.Second)
				fmt.Printf("failed to make connection with peer node %s, retrying...", err.Error())
				retries++
			}

			broker.peerConnections = append(broker.peerConnections, peerConnection)
		}
	}

	if err := broker.restoreBroker(); err != nil {
		log.Fatal("error restoring broker state on startup", err.Error())
	}

	go broker.clearNackMessages()

	return broker
}

func (b *Broker) saveMetadata() error {
	queueConfigs := make(map[string]storage.QueueConfig)
	for key, queue := range b.queues {
		queueConfigs[key] = queue.Config
	}

	var authValue storage.AuthStore

	authValue.Admin = *b.Auth.Admin
	authValue.Users = make(map[string]auth.User)
	for key, user := range b.Auth.Users {
		authValue.Users[key] = *user
	}

	metadataJson, err := json.Marshal(&storage.BrokerMetadata{
		QueueConfigs:   queueConfigs,
		SchemaRegistry: b.schemaRegistry,
		Auth:           authValue,
	})
	if err != nil {
		return err
	}

	if err = b.fileStorage.StoreBrokerMetadata(metadataJson); err != nil {
		return err
	}

	log.Println("broker metadata saved")
	return nil
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

	stateJson, err := json.Marshal(storage.BrokerState{
		Exchanges:   b.exchanges,
		Queues:      queues,
		Bindings:    b.bindings,
		PendingAcks: pendingAcks,
	})

	if err != nil {
		return err
	}

	if err = b.fileStorage.StoreBrokerState(stateJson); err != nil {
		return err
	}

	if b.Config.IsMaster && len(b.Config.PeerNodes) > 0 && b.Auth.Admin != nil {
		go b.broadcastMasterState()
	}

	log.Println("broker state saved")
	return nil
}

func (b *Broker) restoreBroker() error {
	log.Println("restoring broker state on startup")
	brokerState, err := b.fileStorage.GetBrokerState()
	if err != nil {
		return err
	}
	if brokerState == nil {
		log.Println("no broker state in store")
		return nil
	}

	brokerMetadata, err := b.fileStorage.GetBrokerMetadata()
	if err != nil {
		return err
	}
	if brokerMetadata == nil {
		log.Println("no metadata in store")
		return nil
	}

	var brokerAuth auth.Auth
	brokerAuth.Admin = &auth.User{
		Name:   brokerMetadata.Auth.Admin.Name,
		Role:   brokerMetadata.Auth.Admin.Role,
		ApiKey: brokerMetadata.Auth.Admin.ApiKey,
	}
	brokerAuth.Users = make(map[string]*auth.User)
	for key, user := range brokerMetadata.Auth.Users {
		brokerAuth.Users[key] = &auth.User{
			Name:   user.Name,
			Role:   user.Role,
			ApiKey: user.ApiKey,
		}
	}

	queues := make(map[string]*storage.Queue)
	pendingAcks := make(map[string]map[string]*storage.PendingAck)

	for queueName, msgIDs := range brokerState.Queues {
		var config storage.QueueConfig
		if util.MapContains(brokerMetadata.QueueConfigs, queueName) {
			config = brokerMetadata.QueueConfigs[queueName]
		} else {
			log.Printf("no queue config metadata found for %s", queueName)
			return fmt.Errorf("no queue config metadata found for %s", queueName)
		}
		queue := &storage.Queue{
			Name:     queueName,
			Config:   config,
			Messages: make([]*storage.Message, 0),
			Mutex:    sync.Mutex{},
		}

		for _, msgID := range msgIDs {
			msg, err := b.fileStorage.GetMessage(msgID)
			if err != nil {
				return err
			}
			queue.Messages = append(queue.Messages, msg)
		}

		queues[queueName] = queue
	}

	for queueName, msgIDs := range brokerState.PendingAcks {
		for _, msgID := range msgIDs {
			msg, err := b.fileStorage.GetMessage(msgID)
			if err != nil {
				return err
			}
			if !util.MapContains(pendingAcks, queueName) {
				pendingAcks[queueName] = make(map[string]*storage.PendingAck)
			}
			pendingAcks[queueName][msgID] = &storage.PendingAck{
				Message:  msg,
				TimeSent: time.Now(),
			}
		}
	}

	b.exchanges = brokerState.Exchanges
	b.bindings = brokerState.Bindings
	b.queues = queues
	b.pendingAcks = pendingAcks
	b.schemaRegistry = brokerMetadata.SchemaRegistry
	b.Auth = &brokerAuth

	return nil
}

func (b *Broker) CreateAdmin(username string) (string, error) {
	apiKey, err := b.Auth.CreateAdmin(username)
	if err != nil {
		log.Printf("error while creating admin %v", err)
		return "", fmt.Errorf("error while creating admin %v", err)
	}

	if err := b.saveMetadata(); err != nil {
		log.Printf("error saving broker metadata %s, restroing broker state...", err.Error())

		b.Auth.RemoveAdmin()
		return "", err
	}

	return apiKey, nil
}

func (b *Broker) CreateUser(username, role string) (string, error) {
	apiKey, err := b.Auth.CreateUser(username, role)
	if err != nil {
		log.Printf("error while creating admin %v", err)
		return "", fmt.Errorf("error while creating admin %v", err)
	}

	if err := b.saveMetadata(); err != nil {
		log.Printf("error saving broker metadata %s, restroing broker state...", err.Error())

		b.Auth.RemoveUserApiKey(apiKey)
		return "", err
	}

	return apiKey, nil
}

func (b *Broker) RevokeApiKey(apiKey string) error {
	user, err := b.Auth.RemoveUserApiKey(apiKey)
	if err != nil {
		log.Printf("error while creating admin %v", err)
		return fmt.Errorf("error while creating admin %v", err)
	}

	if err := b.saveMetadata(); err != nil {
		log.Printf("error saving broker metadata %s, restroing broker state...", err.Error())

		b.Auth.Users[user.ApiKey] = user
		return err
	}

	return nil
}

func (b *Broker) CreateExchange(name, exchangeType, exchangeSchema string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if util.MapContains(b.exchanges, name) {
		log.Printf("exchange with the name %s already exists", name)
		return fmt.Errorf("exchange with the name %s already exists", name)
	}

	if err := internalutil.ValidateProtobufSchema(exchangeSchema); err != nil {
		log.Printf("error while validating protobuf message schema for payload %v", err)
		return fmt.Errorf("error while validating protobuf message schema for payload %v", err)
	}

	eType, err := storage.NewExchangeType(exchangeType)
	if err != nil {
		log.Printf("exchange type %s is not valid", exchangeType)
		return fmt.Errorf("exchange type %s is not valid", exchangeType)
	}

	b.exchanges[name] = eType
	b.schemaRegistry[name] = exchangeSchema

	metadataErr := b.saveMetadata()
	stateErr := b.saveState()

	if metadataErr != nil || stateErr != nil {
		log.Printf("error saving broker metadata/state %v/%v, undoing broker state...", metadataErr, stateErr)

		delete(b.exchanges, name)
		return fmt.Errorf("error saving broker metadata/state, operation failed")
	}

	log.Printf("exchange %s of type %s created", name, exchangeType)
	return nil
}

func (b *Broker) RemoveExchange(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.exchanges, name) {
		log.Printf("exchange with the name %s does not exists", name)
		return fmt.Errorf("exchange with the name %s does not exists", name)
	}

	exchangeType := b.exchanges[name]
	schemaRegistry := b.schemaRegistry[name]

	var bindings map[string][]string
	var queues map[string]*storage.Queue

	if util.MapContains(b.bindings, name) {
		bindings = b.bindings[name]
		for queueName := range bindings {
			if queues == nil {
				queues = map[string]*storage.Queue{}
			}
			queues[queueName] = b.queues[queueName]
			delete(b.queues, queueName)
		}
	}

	delete(b.exchanges, name)
	delete(b.bindings, name)
	delete(b.schemaRegistry, name)

	metadataErr := b.saveMetadata()
	stateErr := b.saveState()

	if metadataErr != nil || stateErr != nil {
		log.Printf("error saving broker metadata/state %v/%v, undoing broker state...", metadataErr, stateErr)

		b.exchanges[name] = exchangeType
		b.schemaRegistry[name] = schemaRegistry
		b.bindings[name] = bindings
		for queueName, queue := range queues {
			b.queues[queueName] = queue
		}
		return fmt.Errorf("error saving broker metadata/state, operation failed")
	}

	log.Printf("exchange %s of type %s removed", name, exchangeType)
	return nil
}

func (b *Broker) CreateQueue(name string, config storage.QueueConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var dlqName string

	if util.MapContains(b.queues, name) {
		log.Printf("queue with the name %s already exists", name)
		return fmt.Errorf("queue with the name %s already exists", name)
	}

	if config.DLQ {
		dlqName = fmt.Sprintf("%s-dead-letter", name)
		if util.MapContains(b.queues, dlqName) {
			log.Printf("dead letter queue with the name %s already exists", dlqName)
			return fmt.Errorf("dead letter queue with the name %s already exists", dlqName)
		}
	}

	b.queues[name] = &storage.Queue{Name: name, Config: config, Messages: make([]*storage.Message, 0), Mutex: sync.Mutex{}}

	if config.DLQ {
		b.queues[dlqName] = &storage.Queue{Name: dlqName, Config: storage.QueueConfig{DLQ: false, MaxRetries: 0}, Messages: make([]*storage.Message, 0), Mutex: sync.Mutex{}}
	}

	if err := b.saveMetadata(); err != nil {
		log.Printf("error saving broker metadata %s, restroing broker state...", err.Error())

		delete(b.queues, name)
		delete(b.queues, dlqName)
		return err
	}
	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		delete(b.queues, name)
		delete(b.queues, dlqName)
		return err
	}

	log.Printf("queue %s created", name)
	return nil
}

func (b *Broker) RemoveQueue(exchangeName, queueName string) error {
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

func (b *Broker) BindQueue(exchange, queue, routingKey string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.bindings[exchange] == nil {
		b.bindings[exchange] = make(map[string][]string)
	}

	if !util.MapContains(b.queues, queue) {
		log.Printf("queue %s does not exist for exchange %s", queue, exchange)
		return fmt.Errorf("queue %s does not exist for exchange %s", queue, exchange)
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

func (b *Broker) PublishMessage(exchange, routingKey string, msg *storage.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.bindings, exchange) {
		log.Printf("no bindings exist for exchange %s", exchange)
		return fmt.Errorf("no bindings exist for exchange %s", exchange)
	}

	if err := util.UnmarshalBytesToProtobuf(msg.Payload, exchange, b.schemaRegistry[exchange]); err != nil {
		log.Println("error while unmarshaling message payload bytes to protobuf message: ", err.Error())
		return fmt.Errorf("error while unmarshaling message payload bytes to protobuf message: %s", err.Error())
	}

	if b.exchanges[exchange] == storage.Fanout {
		for queue := range b.bindings[exchange] {
			b.queues[queue].Enqueue(msg)
			log.Printf("route-queue %s-%s is enqueued with message %s", routingKey, queue, msg.ID)
		}
	} else if b.exchanges[exchange] == storage.Direct {
		for queue, keys := range b.bindings[exchange] {
			for _, key := range keys {
				if key == routingKey {
					b.queues[queue].Enqueue(msg)
					log.Printf("route-queue %s-%s is enqueued with message %s", routingKey, queue, msg.ID)
				}
			}
		}
	}

	if err := b.fileStorage.StoreMessage(msg); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		for queue, keys := range b.bindings[exchange] {
			for _, key := range keys {
				if key == routingKey {
					b.queues[queue].Dequeue()
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
					b.queues[queue].Dequeue()
				}
			}
		}
		return err
	}

	if b.Config.IsMaster && len(b.Config.PeerNodes) > 0 {
		go b.broadcastMessageToPeers(msg)
	}

	return nil
}

func (b *Broker) broadcastMessageToPeers(msg *storage.Message) {
	log.Println("broadcasting published message to peers...")
	message := &protoc.Message{
		Id:        msg.ID,
		Payload:   msg.Payload,
		Timestamp: msg.Timestamp,
	}
	for _, peer := range b.peerConnections {
		retries := 1
		for {
			if retries > 3 {
				log.Println("failed to broadcast message to peer node after several retries")
			}
			_, err := peer.client.BroadCastMessageToPeer(context.Background(), &protoc.BroadCastMessageToPeerRequest{Message: message})
			if err == nil {
				break
			}
			time.Sleep(time.Duration(math.Pow(2, float64(retries))) * time.Second)
			log.Printf("failed to broadcast message to peer node %s, retrying...", err.Error())
			retries++
		}
	}
}

func (b *Broker) CreateConsumer(queueName string) (*Consumer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.queues, queueName) {
		log.Printf("queue %s does not exist", queueName)
		return nil, fmt.Errorf("queue %s does not exist", queueName)
	}

	consumerID := uuid.New().String()
	consumer := &Consumer{
		ID:      consumerID,
		MsgChan: make(chan *storage.Message),
		Retries: 0,
	}

	if !util.MapContains(b.consumers, queueName) {
		b.consumers[queueName] = &ConsumersList{
			LastIndex: 0,
			Consumers: make([]*Consumer, 0),
		}
	}

	if len(b.consumers[queueName].Consumers) > 0 {
		b.stopConsumerChans[queueName] <- true
	}

	b.consumers[queueName].LastIndex = 0
	b.consumers[queueName].Consumers = append(b.consumers[queueName].Consumers, consumer)

	var stopChan chan bool
	if util.MapContains(b.stopConsumerChans, queueName) {
		stopChan = b.stopConsumerChans[queueName]
	} else {
		stopChan = make(chan bool)
		b.stopConsumerChans[queueName] = stopChan
	}

	go b.consumeMessage(queueName, stopChan)

	return consumer, nil
}

func (b *Broker) moveToDLQ(dlqName string, message *storage.Message) {
	if !util.MapContains(b.queues, dlqName) {
		b.queues[dlqName] = &storage.Queue{
			Name:     dlqName,
			Messages: make([]*storage.Message, 0),
			Mutex:    sync.Mutex{},
		}
	}

	b.queues[dlqName].Messages = append(b.queues[dlqName].Messages, message)
	log.Printf("max retries exceeded, message %s moved to dlq %s", message.ID, dlqName)
}

func (b *Broker) consumeMessage(queueName string, stopChan chan bool) {
	b.mu.Lock()
	dlq := b.queues[queueName].Config.DLQ
	maxRetries := b.queues[queueName].Config.MaxRetries
	b.mu.Unlock()

	for {
		select {
		case <-stopChan:
			log.Printf("queue %s consumer is received stop signal", queueName)
			return
		default:
			message := b.queues[queueName].Dequeue()

			if message != nil {
				retries, found := b.messageRetries[message.ID]
				if !found {
					b.messageRetries[message.ID] = 0
				}
				if found && retries > maxRetries {
					var dlqName string
					if dlq {
						dlqName = fmt.Sprintf("%s-dead-letter", queueName)
						b.moveToDLQ(dlqName, message)
					}
					delete(b.pendingAcks[queueName], message.ID)
					delete(b.messageRetries, message.ID)

					err := b.saveState()
					if err == nil {
						continue
					}

					log.Printf("error saving broker state %s, restroing broker state...", err.Error())
					if dlq {
						b.queues[dlqName].Messages = util.RemoveArrayElement(b.queues[dlqName].Messages, b.queues[dlqName].Size()-1)
					}
				}

				b.messageRetries[message.ID] = retries + 1

				if b.pendingAcks[queueName] == nil {
					b.pendingAcks[queueName] = make(map[string]*storage.PendingAck)
				}
				b.pendingAcks[queueName][message.ID] = &storage.PendingAck{
					Message:  message,
					TimeSent: time.Now(),
				}

				if err := b.saveState(); err != nil {
					log.Printf("error saving broker state %s, restroing broker state...", err.Error())

					b.queues[queueName].Enqueue(message)
					delete(b.pendingAcks[queueName], message.ID)

					continue
				}

				lastIndex := b.consumers[queueName].LastIndex
				nextIndex := (lastIndex + 1) % len(b.consumers[queueName].Consumers)

				consumer := b.consumers[queueName].Consumers[nextIndex]

				select {
				case consumer.MsgChan <- message:
					log.Printf("message %s sent to consumer %s channel", message.ID, consumer.ID)
					log.Printf("message %s is waiting for acknowdegement", message.ID)
				default:
					if consumer.Retries >= uint8(b.Config.ConsumerConnectionRetries) {
						b.consumers[queueName].Consumers = util.RemoveArrayElement(b.consumers[queueName].Consumers, nextIndex)

						if len(b.consumers[queueName].Consumers) == 0 {
							log.Printf("consumer %s is stopped/crashed for queue", consumer.ID)
							return
						}
					}

					consumer.Retries += 1
					log.Printf("consumer %s message channel is closed - Retries %d", consumer.ID, consumer.Retries)
				}

				b.consumers[queueName].LastIndex = nextIndex
			}
		}
	}
}

func (b *Broker) MessageAcknowledge(queueName, msgID string) error {
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
	delete(b.messageRetries, msgID)

	if err := b.saveState(); err != nil {
		log.Printf("error saving broker state %s, restroing broker state...", err.Error())

		b.pendingAcks[queueName][msgID] = pendingAck

		return err
	}

	log.Printf("message %s has been acknowledged", msgID)

	return nil
}

func (b *Broker) clearNackMessages() {
	for {
		b.mu.Lock()
		log.Printf("clearing not acknowledged messages...")
		for queueName, messages := range b.pendingAcks {
			for messageID, pending := range messages {
				if time.Since(pending.TimeSent) > b.ackTimeout {
					b.queues[queueName].Enqueue(pending.Message)
					delete(b.pendingAcks[queueName], messageID)

					log.Printf("message %s has not received acknowledgement and hence re-enqueued", messageID)
				}
			}
		}
		if err := b.saveState(); err != nil {
			b.mu.Unlock()
			log.Printf("error saving broker state %s", err.Error())
		}

		b.mu.Unlock()
		time.Sleep(time.Second * time.Duration(b.Config.MessageNackClearInterval))
	}
}

func (b *Broker) RetrieveMessages(queueName string, n int) ([]*storage.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if util.MapContains(b.queues, queueName) {
		return b.queues[queueName].Peek(n), nil
	}

	log.Printf("queue %s does not exist", queueName)
	return nil, fmt.Errorf("queue %s does not exist", queueName)
}

func (b *Broker) RedriveDlqMessages(queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	dlqName := fmt.Sprintf("%s-dead-letter", queueName)

	if !util.MapContains(b.queues, queueName) {
		log.Printf("queue %s does not exist", queueName)
		return fmt.Errorf("queue %s does not exist", queueName)
	}
	if !util.MapContains(b.queues, dlqName) {
		log.Printf("DLQ for queue %s does not exist", queueName)
		return fmt.Errorf("DLQ for queue %s does not exist", queueName)
	}

	for b.queues[dlqName].Size() > 0 {
		message := b.queues[dlqName].Dequeue()
		b.queues[queueName].Enqueue(message)
	}

	return nil
}

func (b *Broker) GetExchangeSchema(exchangeName string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !util.MapContains(b.schemaRegistry, exchangeName) {
		log.Printf("exchange %s does not exist", exchangeName)
		return "", fmt.Errorf("exchange %s does not exist", exchangeName)
	}

	return b.schemaRegistry[exchangeName], nil
}

func (b *Broker) ProcessBroadcastedMessage(msg *storage.Message) error {
	return b.fileStorage.StoreMessage(msg)
}

func (b *Broker) broadcastMasterState() {
	log.Printf("broadcasting master state to cluster nodes...")

	masterState := &protoc.SyncClusterStateRequest{
		AuthStore:      nil,
		Exchanges:      nil,
		SchemaRegistry: nil,
		QueueConfigs:   nil,
		Queues:         nil,
		Bindings:       nil,
	}

	authStore := &protoc.AuthStore{}
	authStore.Admin = &protoc.User{
		Name:   b.Auth.Admin.Name,
		Role:   int32(b.Auth.Admin.Role),
		ApiKey: b.Auth.Admin.ApiKey,
	}
	users := make(map[string]*protoc.User)
	for apiKey, user := range b.Auth.Users {
		users[apiKey] = &protoc.User{
			Name:   user.Name,
			Role:   int32(user.Role),
			ApiKey: user.ApiKey,
		}
	}
	authStore.Users = users
	masterState.AuthStore = authStore

	exchanges := make(map[string]int32)
	for exchangeName, exchangeType := range b.exchanges {
		exchanges[exchangeName] = int32(exchangeType)
	}
	masterState.Exchanges = exchanges

	schemaRegistry := make(map[string]string)
	for exchangeName, exchangeSchema := range b.schemaRegistry {
		schemaRegistry[exchangeName] = exchangeSchema
	}
	masterState.SchemaRegistry = schemaRegistry

	queues := make(map[string]*protoc.StringList)
	queueConfigs := make(map[string]*protoc.QueueConfig)
	for queueName, queue := range b.queues {
		queueConfigs[queueName] = &protoc.QueueConfig{
			Dlq:        queue.Config.DLQ,
			MaxRetries: int32(queue.Config.MaxRetries),
		}

		msgIDs := make([]string, 0)
		for _, message := range queue.Messages {
			msgIDs = append(msgIDs, message.ID)
		}

		queues[queueName] = &protoc.StringList{Elements: msgIDs}
	}
	masterState.Queues = queues
	masterState.QueueConfigs = queueConfigs

	bindings := make(map[string]*protoc.Bindings)
	for exchangeName, binding := range b.bindings {
		queueBindings := make(map[string]*protoc.StringList)

		for route, bindedQueues := range binding {
			queueBindings[route] = &protoc.StringList{Elements: bindedQueues}
		}

		bindings[exchangeName] = &protoc.Bindings{
			QueueBindings: queueBindings,
		}
	}
	masterState.Bindings = bindings

	for _, peer := range b.peerConnections {
		retries := 1
		for {
			if retries > 3 {
				log.Println("failed to broadcast state to peer node after several retries")
			}
			_, err := peer.client.SyncCluster(context.Background(), masterState)
			if err == nil {
				break
			}
			time.Sleep(time.Duration(math.Pow(2, float64(retries))) * time.Second)
			log.Printf("failed to broadcast state to peer node %s, retrying...", err.Error())
			retries++
		}
	}
}

func (b *Broker) SyncWithMasterState(authStore *auth.Auth, exchanges map[string]storage.ExchangeType, schemaRegistry map[string]string, queues map[string][]string, queueConfigs map[string]storage.QueueConfig, bindings map[string]map[string][]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.Auth = authStore
	b.exchanges = exchanges
	b.schemaRegistry = schemaRegistry
	b.bindings = bindings

	_queues := make(map[string]*storage.Queue)
	for queueName, configs := range queueConfigs {
		var messages []*storage.Message

		msgIDs := queues[queueName]
		for _, msgID := range msgIDs {
			message, err := b.fileStorage.GetMessage(msgID)
			if err != nil {
				continue
			}
			messages = append(messages, message)
		}

		_queues[queueName] = &storage.Queue{
			Name: queueName,
			Config: storage.QueueConfig{
				DLQ:        configs.DLQ,
				MaxRetries: configs.MaxRetries,
			},
			Messages: messages,
			Mutex:    sync.Mutex{},
		}
	}

	b.queues = _queues

	b.saveMetadata()

	return nil
}
