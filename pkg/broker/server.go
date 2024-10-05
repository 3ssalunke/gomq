package broker

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/3ssalunke/gomq/pkg/protoc"
)

type BrokerServiceServer struct {
	protoc.UnimplementedBrokerServiceServer
	Broker *Broker
}

func (s *BrokerServiceServer) CreateExchange(ctx context.Context, req *protoc.Exchange) (*protoc.BrokerResponse, error) {
	exchangeName := strings.TrimSpace(req.Name)
	exchangeType := strings.TrimSpace(req.Type)
	exchangeSchema := strings.TrimSpace(req.Schema)

	if exchangeName == "" || exchangeType == "" || exchangeSchema == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.createExchange(exchangeName, exchangeType, exchangeSchema); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Status: true, Message: fmt.Sprintf("exchange %s of type %s created", exchangeName, exchangeType)}, nil
}

func (s *BrokerServiceServer) RemoveExchange(ctx context.Context, req *protoc.RemoveExchangeRequest) (*protoc.BrokerResponse, error) {
	exchangeName := strings.TrimSpace(req.ExchangeName)

	if exchangeName == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.removeExchange(exchangeName); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Status: true, Message: fmt.Sprintf("exchange %s removed", exchangeName)}, nil
}

func (s *BrokerServiceServer) CreateQueue(ctx context.Context, req *protoc.Queue) (*protoc.BrokerResponse, error) {
	queueName := strings.TrimSpace(req.Name)
	queueConfig := QueueConfig{
		DLQ:        req.Dlq,
		MaxRetries: int8(req.MaxRetries),
	}

	if queueName == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.createQueue(queueName, queueConfig); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Status: true, Message: fmt.Sprintf("queue %s created", queueName)}, nil
}

func (s *BrokerServiceServer) RemoveQueue(ctx context.Context, req *protoc.RemoveQueueRequest) (*protoc.BrokerResponse, error) {
	exchangeName := strings.TrimSpace(req.ExchangeName)
	queueName := strings.TrimSpace(req.QueueName)

	if queueName == "" || exchangeName == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.removeQueue(exchangeName, queueName); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Status: true, Message: fmt.Sprintf("queue %s removed from exchange %s", queueName, exchangeName)}, nil
}

func (s *BrokerServiceServer) BindQueue(ctx context.Context, req *protoc.Binding) (*protoc.BrokerResponse, error) {
	exchange := strings.TrimSpace(req.Exchange)
	queue := strings.TrimSpace(req.Queue)
	routingKey := strings.TrimSpace(req.RoutingKey)

	if exchange == "" || queue == "" || routingKey == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.bindQueue(req.Exchange, req.Queue, req.RoutingKey); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Status: true, Message: fmt.Sprintf("queue %s is binded to exchange %s by routing key %s", queue, exchange, routingKey)}, nil
}

func (s *BrokerServiceServer) PublishMessage(ctx context.Context, req *protoc.PublishMessageRequest) (*protoc.BrokerResponse, error) {
	exchange := strings.TrimSpace(req.Exchange)
	routingKey := strings.TrimSpace(req.RoutingKey)

	if exchange == "" || routingKey == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	err := s.Broker.publishMessage(req.Exchange, req.RoutingKey, &Message{
		ID:        req.Message.Id,
		Payload:   req.Message.Payload,
		Timestamp: req.Message.Timestamp,
	})
	if err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Status: true, Message: fmt.Sprintf("message published to exchange %s with routing key %s", exchange, routingKey)}, nil
}

func (s *BrokerServiceServer) RetrieveMessages(ctx context.Context, req *protoc.RetrieveMessagesRequest) (*protoc.RetrieveMessagesResponse, error) {
	queue := strings.TrimSpace(req.Queue)
	count := req.Count

	if queue == "" || count <= 0 {
		return nil, fmt.Errorf("invalid request arguments")
	}

	queueMessages, err := s.Broker.retrieveMessages(req.Queue, int(req.Count))
	if err != nil {
		return nil, err
	}
	pbMessages := []*protoc.Message{}
	for _, msg := range queueMessages {
		pbMessage := &protoc.Message{
			Id:        msg.ID,
			Payload:   msg.Payload,
			Timestamp: msg.Timestamp,
		}
		pbMessages = append(pbMessages, pbMessage)
	}

	return &protoc.RetrieveMessagesResponse{
		Status:   true,
		Messages: pbMessages,
		Message:  fmt.Sprintf("%d messages retrieved from queue %s", len(pbMessages), queue),
	}, nil
}

func (s *BrokerServiceServer) ConsumeMessages(req *protoc.Queue, stream protoc.BrokerService_ConsumeMessagesServer) error {
	queueName := strings.TrimSpace(req.Name)

	if queueName == "" {
		return fmt.Errorf("invalid request arguments")
	}

	consumer, err := s.Broker.createConsumer(queueName)
	if err != nil {
		return err
	}

	for msg := range consumer.MsgChan {
		if msg != nil {
			err := stream.Send(&protoc.Message{
				Id:        msg.ID,
				Payload:   msg.Payload,
				Timestamp: msg.Timestamp,
			})
			if err != nil {
				log.Println(err)
				return err
			}
		}
	}

	return fmt.Errorf("message channel for consumer %s closed", consumer.ID)
}

func (s *BrokerServiceServer) MessageAcknowledge(ctx context.Context, req *protoc.MessageAckRequest) (*protoc.BrokerResponse, error) {
	queueName := strings.TrimSpace(req.Queue)
	msgID := strings.TrimSpace(req.MesssageId)

	if queueName == "" || msgID == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.messageAcknowledge(queueName, msgID); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{
		Status:  true,
		Message: fmt.Sprintf("message %s has been acknowledged", msgID),
	}, nil
}

func (s *BrokerServiceServer) RedriveDlqMessages(ctx context.Context, req *protoc.Queue) (*protoc.BrokerResponse, error) {
	queueName := strings.TrimSpace(req.Name)

	if queueName == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.redriveDlqMessages(queueName); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{
		Status:  true,
		Message: fmt.Sprintf("Messages redrived to %s", queueName),
	}, nil
}

func (s *BrokerServiceServer) GetExchangeSchema(ctx context.Context, req *protoc.GetExchangeSchemaRequest) (*protoc.GetExchangeSchemaResponse, error) {
	exchangeName := strings.TrimSpace(req.ExchangeName)
	if exchangeName == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	schema, err := s.Broker.GetExchangeSchema(exchangeName)
	if err != nil {
		return nil, err
	}

	return &protoc.GetExchangeSchemaResponse{
		Status:  true,
		Message: fmt.Sprintf("schema for exchange %s retrieved", exchangeName),
		Schema:  schema,
	}, nil
}
