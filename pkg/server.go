package pkg

import (
	"context"
	"time"

	"github.com/3ssalunke/gomq/pkg/protoc"
)

type BrokerServiceServer struct {
	protoc.UnimplementedBrokerServiceServer
	Broker *Broker
}

func (s *BrokerServiceServer) CreateQueue(ctx context.Context, req *protoc.CreateQueueRequest) (*protoc.CreateQueueResponse, error) {
	s.Broker.CreateQueue(req.Name)
	return &protoc.CreateQueueResponse{Status: "created"}, nil
}

func (s *BrokerServiceServer) Publish(ctx context.Context, req *protoc.PublishRequest) (*protoc.PublishResponse, error) {
	msg := req.Message
	s.Broker.Publish(req.Queue, &Message{
		ID:        msg.Id,
		Payload:   []byte(msg.Payload),
		Timestamp: msg.Timestamp,
	})
	return &protoc.PublishResponse{Status: "published"}, nil
}

func (s *BrokerServiceServer) Subscribe(req *protoc.SubscribeRequest, stream protoc.BrokerService_SubscribeServer) error {
	go func() {
		for {
			msg := s.Broker.Subscribe(req.Queue)
			if msg != nil {
				stream.Send(&protoc.SubscribeResponse{
					Message: &protoc.Message{
						Id:        msg.ID,
						Payload:   string(msg.Payload),
						Timestamp: msg.Timestamp,
					},
				})
			}
			time.Sleep(time.Second)
		}
	}()
	return nil
}
