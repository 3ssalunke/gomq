package main

import (
	"context"
	"fmt"
	"log"

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Consume(ctx, &protoc.Queue{Name: "TestQueue"})
	if err != nil {
		log.Fatal("err", err.Error())
	}

	for {
		msg, err := stream.Recv()

		if err != nil {
			log.Fatal(err.Error())
		}
		fmt.Println(msg.Payload)
	}
}
