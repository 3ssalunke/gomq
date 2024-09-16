package client

import (
	"context"
	"log"
	"time"

	"github.com/3ssalunke/gomq/pkg/protoc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func createClient() (*grpc.ClientConn, protoc.BrokerServiceClient, error) {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return conn, protoc.NewBrokerServiceClient(conn), nil
}

func CreateExchange(name string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	res, err := client.CreateExchange(context.TODO(), &protoc.Exchange{Name: name})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func CreateQueue(name string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	res, err := client.CreateQueue(context.TODO(), &protoc.Queue{Name: name})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func BindQueue(exchangeName, queueName, routingKey string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	res, err := client.BindQueue(context.TODO(), &protoc.Binding{Exchange: exchangeName, Queue: queueName, RoutingKey: routingKey})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func PublishMessage(exchangeName, routingKey, message string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	encodedMessag := &protoc.Message{
		Id:        "00000",
		Payload:   message,
		Timestamp: time.Now().UnixMilli(),
	}
	res, err := client.Publish(context.TODO(), &protoc.PublishRequest{Exchange: exchangeName, RoutingKey: routingKey, Message: encodedMessag})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func StartConsumer(queueName string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Consume(ctx, &protoc.Queue{Name: queueName})
	if err != nil {
		return "", err
	}

	for {
		msg, err := stream.Recv()

		if err != nil {
			return "", err
		}
		log.Println(msg.Payload)
	}
}
