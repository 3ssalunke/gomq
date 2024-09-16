package broker

import (
	"context"
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

func (s *BrokerServiceServer) Publish(ctx context.Context, req *protoc.PublishRequest) (*protoc.BrokerResponse, error) {
	s.Broker.Publish(req.Exchange, req.RoutingKey, &Message{
		ID:        req.Message.Id,
		Payload:   req.Message.Payload,
		Timestamp: req.Message.Timestamp,
	})
	return &protoc.BrokerResponse{Message: "message published"}, nil
}

func (s *BrokerServiceServer) Consume(req *protoc.Queue, stream protoc.BrokerService_ConsumeServer) error {
	for {
		msg := s.Broker.Consume(req.Name)
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
