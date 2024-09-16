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

	res, err := client.CreateExchange(context.TODO(), &protoc.Exchange{Name: "TestExchange"})
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(res.Message)

	res, err = client.CreateQueue(context.TODO(), &protoc.Queue{Name: "TestQueue"})
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(res.Message)

	res, err = client.BindQueue(context.TODO(), &protoc.Binding{Exchange: "TestExchange", Queue: "TestQueue", RoutingKey: "Test"})
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(res.Message)

	message := &protoc.Message{
		Id:        "00000",
		Payload:   "hello",
		Timestamp: time.Now().UnixMilli(),
	}
	res, err = client.Publish(context.TODO(), &protoc.PublishRequest{Exchange: "TestExchange", RoutingKey: "Test", Message: message})
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(res.Message)
}
