package broker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/3ssalunke/gomq/pkg/protoc"
)

type BrokerServiceServer struct {
	protoc.UnimplementedBrokerServiceServer
	Broker *Broker
}

func (s *BrokerServiceServer) CreateExchange(ctx context.Context, req *protoc.Exchange) (*protoc.BrokerResponse, error) {
	exchangeName := strings.TrimSpace(req.Name)
	exchangeType := strings.TrimSpace(req.Type)

	if exchangeName == "" || exchangeType == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.CreateExchange(exchangeName, exchangeType); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Message: fmt.Sprintf("exchange %s of type %s created", exchangeName, exchangeType)}, nil
}

func (s *BrokerServiceServer) CreateQueue(ctx context.Context, req *protoc.Queue) (*protoc.BrokerResponse, error) {
	queueName := strings.TrimSpace(req.Name)

	if queueName == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.CreateQueue(req.Name); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Message: fmt.Sprintf("queue %s created", queueName)}, nil
}

func (s *BrokerServiceServer) BindQueue(ctx context.Context, req *protoc.Binding) (*protoc.BrokerResponse, error) {
	exchange := strings.TrimSpace(req.Exchange)
	queue := strings.TrimSpace(req.Queue)
	routingKey := strings.TrimSpace(req.RoutingKey)

	if exchange == "" || queue == "" || routingKey == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	if err := s.Broker.BindQueue(req.Exchange, req.Queue, req.RoutingKey); err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Message: fmt.Sprintf("queue %s is binded to exchange %s by routing key %s", queue, exchange, routingKey)}, nil
}

func (s *BrokerServiceServer) PublishMessage(ctx context.Context, req *protoc.PublishMessageRequest) (*protoc.BrokerResponse, error) {
	exchange := strings.TrimSpace(req.Exchange)
	routingKey := strings.TrimSpace(req.RoutingKey)

	if exchange == "" || routingKey == "" {
		return nil, fmt.Errorf("invalid request arguments")
	}

	err := s.Broker.PublishMessage(req.Exchange, req.RoutingKey, &Message{
		ID:        req.Message.Id,
		Payload:   req.Message.Payload,
		Timestamp: req.Message.Timestamp,
	})
	if err != nil {
		return nil, err
	}

	return &protoc.BrokerResponse{Message: fmt.Sprintf("message published to exchange %s with routing key %s", exchange, routingKey)}, nil
}

func (s *BrokerServiceServer) RetrieveMessages(ctx context.Context, req *protoc.RetrieveMessagesRequest) (*protoc.RetrieveMessagesResponse, error) {
	queue := strings.TrimSpace(req.Queue)
	count := req.Count

	if queue == "" || count <= 0 {
		return nil, fmt.Errorf("invalid request arguments")
	}

	queueMessages, err := s.Broker.RetrieveMessages(req.Queue, int(req.Count))
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
		Messages: pbMessages,
		Message:  fmt.Sprintf("%d messages retrieved from queue %s", len(pbMessages), queue),
	}, nil
}

func (s *BrokerServiceServer) ConsumeMessages(req *protoc.Queue, stream protoc.BrokerService_ConsumeMessagesServer) error {
	queue := strings.TrimSpace(req.Name)

	if queue == "" {
		return fmt.Errorf("invalid request arguments")
	}

	for {
		msg, err := s.Broker.ConsumeMessage(req.Name)
		if err != nil {
			return err
		}
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
		} else {
			time.Sleep(time.Second)
		}
	}
}
