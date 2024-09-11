package main

import (
	"context"
	"log"
	"time"

	"github.com/3ssalunke/gomq/pkg/protoc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	client := protoc.NewBrokerServiceClient(conn)

	client.CreateQueue(context.TODO(), &protoc.CreateQueueRequest{Name: "Testqueue"})
	message := &protoc.Message{
		Id:        "00000",
		Payload:   "hello",
		Timestamp: time.Now().UnixMilli(),
	}
	client.Publish(context.TODO(), &protoc.PublishRequest{Queue: "Testqueue", Message: message})
}
