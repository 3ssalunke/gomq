package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/3ssalunke/gomq/pkg/protoc"
)

type BrokerServiceServer struct {
	protoc.UnimplementedBrokerServiceServer
	Broker *Broker
}

func (s *BrokerServiceServer) CreateExchange(ctx context.Context, req *protoc.Exchange) (*protoc.BrokerResponse, error) {
	s.Broker.CreateExchange(req.Name, req.Type)
	return &protoc.BrokerResponse{Message: "exchange created"}, nil
}

func (s *BrokerServiceServer) CreateQueue(ctx context.Context, req *protoc.Queue) (*protoc.BrokerResponse, error) {
	s.Broker.CreateQueue(req.Name)
	return &protoc.BrokerResponse{Message: "queue created"}, nil
}

func (s *BrokerServiceServer) BindQueue(ctx context.Context, req *protoc.Binding) (*protoc.BrokerResponse, error) {
	s.Broker.BindQueue(req.Exchange, req.Queue, req.RoutingKey)
	return &protoc.BrokerResponse{Message: "queue binded"}, nil
}

func (s *BrokerServiceServer) PublishMessage(ctx context.Context, req *protoc.PublishMessageRequest) (*protoc.BrokerResponse, error) {
	s.Broker.PublishMessage(req.Exchange, req.RoutingKey, &Message{
		ID:        req.Message.Id,
		Payload:   req.Message.Payload,
		Timestamp: req.Message.Timestamp,
	})
	return &protoc.BrokerResponse{Message: "message published"}, nil
}

func (s *BrokerServiceServer) RetrieveMessages(ctx context.Context, req *protoc.RetrieveMessagesRequest) (*protoc.RetrieveMessagesResponse, error) {
	queueMessages := s.Broker.RetrieveMessages(req.Queue, int(req.Count))
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
		Message:  fmt.Sprintf("%d messages retrieved", len(pbMessages)),
	}, nil
}

func (s *BrokerServiceServer) ConsumeMessages(req *protoc.Queue, stream protoc.BrokerService_ConsumeMessagesServer) error {
	for {
		msg := s.Broker.ConsumeMessage(req.Name)
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
